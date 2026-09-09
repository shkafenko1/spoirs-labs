"""ЛР №6: одноранговый чат по UDP (широковещательный + многоадресный режим).

Возможности:
- отправка/приём сообщений через broadcast И multicast одновременно;
- обнаружение и список запущенных приложений (периодические HELLO-маяки);
- самостоятельный выход из multicast-группы (/leave) и обратное вступление (/join);
- принудительное игнорирование хоста локально (/ignore <ip>);
- автоопределение параметров интерфейса (ip, маска, broadcast).

Команды в чате:
    /who            показать активных участников
    /net            показать сетевые параметры интерфейса
    /nick <имя>     сменить имя
    /leave          выйти из multicast-группы
    /join           снова вступить в multicast-группу
    /ignore <ip>    игнорировать сообщения от хоста
    /unignore <ip>  снять игнорирование
    /quit           выход
Любой другой ввод рассылается всем участникам.

Запуск (на нескольких машинах в одной сети):
    python3 lab6/chat.py --nick alice
"""
import argparse
import json
import selectors
import socket
import sys
import threading
import time
import uuid

import netinfo

MULTICAST_GROUP = "239.255.13.37"
DEFAULT_PORT = 50000
BEACON_INTERVAL = 5.0
PEER_TTL = 15.0


def make_broadcast_socket(port: int) -> socket.socket:
    """UDP-сокет для широковещательной отправки и приёма."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    _enable_reuseport(s)
    s.bind(("", port))
    return s


def make_multicast_socket(port: int) -> socket.socket:
    """UDP-сокет, вступивший в multicast-группу."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    _enable_reuseport(s)
    s.bind(("", port))
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    return s


def _enable_reuseport(s: socket.socket) -> None:
    """Разрешает несколько слушателей на одном порту (если ОС умеет)."""
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except OSError:
            pass


def join_group(sock: socket.socket, group: str) -> None:
    """Вступает в multicast-группу (IP_ADD_MEMBERSHIP)."""
    mreq = socket.inet_aton(group) + socket.inet_aton("0.0.0.0")
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)


def leave_group(sock: socket.socket, group: str) -> None:
    """Выходит из multicast-группы (IP_DROP_MEMBERSHIP)."""
    mreq = socket.inet_aton(group) + socket.inet_aton("0.0.0.0")
    try:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
    except OSError:
        pass


class ChatPeer:
    """Состояние одного участника чата (сокеты, список пиров, игнор-лист)."""

    def __init__(self, nick: str, port: int):
        self.nick = nick
        self.port = port
        self.id = uuid.uuid4().hex[:8]  # уникальный идентификатор экземпляра
        self.info = netinfo.interface_info()
        self.bcast = make_broadcast_socket(port)
        self.mcast = make_multicast_socket(port)
        join_group(self.mcast, MULTICAST_GROUP)
        self.in_group = True
        self.peers = {}          # ip -> {"nick": str, "seen": float}
        self.ignored = set()     # игнорируемые ip
        self.running = True
        self.lock = threading.Lock()

    def close(self) -> None:
        """Сообщает о выходе и закрывает сокеты."""
        self.send({"type": "BYE", "nick": self.nick})
        leave_group(self.mcast, MULTICAST_GROUP)
        self.bcast.close()
        self.mcast.close()

    def send(self, message: dict) -> None:
        """Рассылает сообщение и по broadcast, и по multicast."""
        message["src"] = self.id
        data = json.dumps(message).encode("utf-8")
        self.bcast.sendto(data, (self.info["broadcast"], self.port))
        if self.in_group:
            self.mcast.sendto(data, (MULTICAST_GROUP, self.port))


def _remember_peer(peer: ChatPeer, ip: str, nick: str) -> None:
    """Обновляет запись о пире в таблице обнаружения."""
    with peer.lock:
        peer.peers[ip] = {"nick": nick, "seen": time.monotonic()}


def _handle_datagram(peer: ChatPeer, data: bytes, addr) -> None:
    """Обрабатывает один принятый датаграмм."""
    ip = addr[0]
    if ip in peer.ignored:
        return
    try:
        msg = json.loads(data.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return
    if msg.get("src") == peer.id:  # своё же сообщение (эхо broadcast/multicast)
        return
    _remember_peer(peer, ip, msg.get("nick", "?"))
    _dispatch_message(peer, ip, msg)


def _dispatch_message(peer: ChatPeer, ip: str, msg: dict) -> None:
    """Разные типы сообщений: HELLO (тихо), BYE, MSG."""
    kind = msg.get("type")
    if kind == "MSG":
        print(f"\n{msg.get('nick', '?')}@{ip}: {msg.get('text', '')}")
    elif kind == "BYE":
        with peer.lock:
            peer.peers.pop(ip, None)
        print(f"\n* {msg.get('nick', '?')}@{ip} вышел из чата")


def receive_loop(peer: ChatPeer) -> None:
    """Читает оба сокета через selectors в одном потоке."""
    sel = selectors.DefaultSelector()
    sel.register(peer.bcast, selectors.EVENT_READ)
    sel.register(peer.mcast, selectors.EVENT_READ)
    while peer.running:
        for key, _ in sel.select(timeout=0.5):
            data, addr = key.fileobj.recvfrom(65535)
            _handle_datagram(peer, data, addr)
    sel.close()


def beacon_loop(peer: ChatPeer) -> None:
    """Периодически рассылает HELLO и чистит устаревших пиров."""
    while peer.running:
        peer.send({"type": "HELLO", "nick": peer.nick})
        _expire_peers(peer)
        time.sleep(BEACON_INTERVAL)


def _expire_peers(peer: ChatPeer) -> None:
    """Удаляет пиров, от которых давно не было маяка."""
    now = time.monotonic()
    with peer.lock:
        stale = [ip for ip, p in peer.peers.items() if now - p["seen"] > PEER_TTL]
        for ip in stale:
            del peer.peers[ip]
PEER_COMMANDS_HELP = "команды: /who /net /nick /leave /join /ignore /unignore /quit"


def cmd_who(peer: ChatPeer, _arg: str) -> None:
    """Выводит список активных участников (обнаруженных приложений)."""
    with peer.lock:
        items = list(peer.peers.items())
    print(f"Активны ({len(items)}):")
    for ip, p in items:
        print(f"  {p['nick']:15s} {ip}")


def cmd_net(peer: ChatPeer, _arg: str) -> None:
    """Показывает параметры сетевого интерфейса."""
    i = peer.info
    print(f"ip={i['ip']} netmask={i['netmask']} broadcast={i['broadcast']} "
          f"multicast={MULTICAST_GROUP}")


def cmd_nick(peer: ChatPeer, arg: str) -> None:
    """Меняет имя участника."""
    if arg:
        peer.nick = arg
        print(f"* новое имя: {arg}")


def cmd_leave(peer: ChatPeer, _arg: str) -> None:
    """Самостоятельный выход из multicast-группы."""
    if peer.in_group:
        leave_group(peer.mcast, MULTICAST_GROUP)
        peer.in_group = False
        print("* вы покинули multicast-группу (broadcast продолжает работать)")


def cmd_join(peer: ChatPeer, _arg: str) -> None:
    """Повторное вступление в multicast-группу."""
    if not peer.in_group:
        join_group(peer.mcast, MULTICAST_GROUP)
        peer.in_group = True
        print("* вы снова в multicast-группе")


def cmd_ignore(peer: ChatPeer, arg: str) -> None:
    """Принудительно игнорировать сообщения от хоста."""
    if arg:
        peer.ignored.add(arg)
        print(f"* игнорируется: {arg}")


def cmd_unignore(peer: ChatPeer, arg: str) -> None:
    """Снять игнорирование хоста."""
    peer.ignored.discard(arg)
    print(f"* снято игнорирование: {arg}")


COMMANDS = {
    "/who": cmd_who, "/net": cmd_net, "/nick": cmd_nick,
    "/leave": cmd_leave, "/join": cmd_join,
    "/ignore": cmd_ignore, "/unignore": cmd_unignore,
}


def handle_input(peer: ChatPeer, line: str) -> None:
    """Разбирает строку ввода: команда или обычное сообщение."""
    line = line.strip()
    if not line:
        return
    if not line.startswith("/"):
        peer.send({"type": "MSG", "nick": peer.nick, "text": line})
        return
    name, _, arg = line.partition(" ")
    handler = COMMANDS.get(name)
    if handler:
        handler(peer, arg.strip())
    else:
        print(PEER_COMMANDS_HELP)


def input_loop(peer: ChatPeer) -> None:
    """Основной цикл чтения stdin (главный поток)."""
    print(f"Чат запущен как '{peer.nick}'. {PEER_COMMANDS_HELP}")
    cmd_net(peer, "")
    for line in sys.stdin:
        if line.strip() == "/quit":
            break
        handle_input(peer, line)
    peer.running = False


def build_parser() -> argparse.ArgumentParser:
    """Разбор аргументов."""
    p = argparse.ArgumentParser(description="Одноранговый UDP-чат (ЛР6)")
    p.add_argument("--nick", default=socket.gethostname(), help="имя участника")
    p.add_argument("--port", type=int, default=DEFAULT_PORT, help="UDP-порт")
    return p


def main(argv=None) -> int:
    """Точка входа: поднимает потоки приёма/маяка и читает ввод."""
    args = build_parser().parse_args(argv)
    peer = ChatPeer(args.nick, args.port)
    threading.Thread(target=receive_loop, args=(peer,), daemon=True).start()
    threading.Thread(target=beacon_loop, args=(peer,), daemon=True).start()
    try:
        input_loop(peer)
    except KeyboardInterrupt:
        peer.running = False
    finally:
        peer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
