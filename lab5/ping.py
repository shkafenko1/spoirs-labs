"""ЛР №5: параллельный ping нескольких хостов + traceroute.

- Один поток на каждый хост.
- Все потоки читают из ОДНОГО raw-сокета, используя MSG_PEEK: пакет удаляется
  из буфера только после того, как поток убедился, что ответ адресован именно ему
  (по ICMP id). Чужие пакеты остаются в буфере для других потоков.
- Метка времени отправки кладётся в тело пакета, RTT считается по ней.
- Разные обработчики для echo-reply, "время жизни истекло" и "хост недостижим".
- Режим traceroute: определение пути до узла ростом TTL.

Запуск (нужны права root):
    sudo python3 lab5/ping.py 8.8.8.8 1.1.1.1 ya.ru
    sudo python3 lab5/ping.py --trace 8.8.8.8
"""
import argparse
import os
import socket
import sys
import threading
import time

import net

# Общий сокет и блокировка для атомарного peek+consume между потоками.
_recv_lock = threading.Lock()
# Набор "живых" ident-ов, чтобы отбрасывать бесхозные пакеты из буфера.
_active_ids = set()


def resolve(host: str) -> str:
    """Преобразует имя хоста в IPv4-адрес."""
    return socket.gethostbyname(host)


def _peek_consume_if_mine(sock: socket.socket, my_id: int):
    """Пробует прочитать пакет; забирает его только если он адресован нам.

    Возвращает разобранный ICMP-словарь, если пакет наш, иначе None.
    """
    with _recv_lock:
        try:
            packet = sock.recv(2048, socket.MSG_PEEK)
        except socket.timeout:
            return None
        info = net.parse_icmp(packet)
        target_id = _packet_target_id(info)
        if target_id == my_id:
            sock.recv(2048)  # окончательно удаляем ИЗ буфера — пакет наш
            return info
        if target_id not in _active_ids:
            sock.recv(2048)  # бесхозный/устаревший пакет — убираем, чтобы не мешал
        return None


def _packet_target_id(info: dict) -> int:
    """Определяет, какому потоку (ident) принадлежит ICMP-сообщение."""
    if info["type"] == net.ICMP_ECHO_REPLY:
        return info["id"]
    if info["type"] in (net.ICMP_TIME_EXCEEDED, net.ICMP_DEST_UNREACH):
        return net.original_ident_from_error(info["body"])
    return -1


def _wait_reply(sock: socket.socket, my_id: int, deadline: float):
    """Ждёт наш ответ до deadline, уступая процессор другим потокам."""
    while time.time() < deadline:
        info = _peek_consume_if_mine(sock, my_id)
        if info is not None:
            return info
        time.sleep(0.001)
    return None


def _report_reply(host: str, info: dict) -> None:
    """Обработчик успешного echo-reply."""
    rtt = net.rtt_ms_from_payload(info["body"])
    print(f"[{host}] ответ от {info['ip']['src']}: seq={info['seq']} время={rtt:.2f} мс")


def _report_error(host: str, info: dict) -> None:
    """Обработчики для 'TTL истёк' и 'хост недостижим'."""
    src = info["ip"]["src"]
    if info["type"] == net.ICMP_TIME_EXCEEDED:
        print(f"[{host}] время жизни истекло на узле {src}")
    else:
        print(f"[{host}] хост недостижим (от {src}, code={info['code']})")


def ping_host(sock: socket.socket, host: str, ident: int, count: int, timeout: float) -> None:
    """Пингует один хост count раз через ОБЩИЙ сокет (тело одного потока)."""
    addr = resolve(host)
    _active_ids.add(ident)
    try:
        for seq in range(1, count + 1):
            sock.sendto(net.build_echo_request(ident, seq), (addr, 0))
            info = _wait_reply(sock, ident, time.time() + timeout)
            if info is None:
                print(f"[{host}] превышен интервал ожидания (seq={seq})")
            elif info["type"] == net.ICMP_ECHO_REPLY:
                _report_reply(host, info)
            else:
                _report_error(host, info)
            time.sleep(0.2)
    finally:
        _active_ids.discard(ident)


def parallel_ping(hosts: list, count: int, timeout: float) -> None:
    """Открывает один общий сокет и запускает по потоку на каждый хост."""
    sock = net.make_icmp_socket(timeout)
    threads = []
    for i, host in enumerate(hosts):
        ident = (os.getpid() + i) & 0xFFFF
        t = threading.Thread(target=ping_host, args=(sock, host, ident, count, timeout))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    sock.close()


def _trace_probe(sock: socket.socket, addr: str, ident: int, ttl: int, timeout: float):
    """Отправляет один зонд с заданным TTL, ждёт ответ (echo-reply или ошибку)."""
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_TTL, ttl)
    sock.sendto(net.build_echo_request(ident, ttl), (addr, 0))
    return _wait_reply(sock, ident, time.time() + timeout)


def traceroute(host: str, max_hops: int, timeout: float) -> None:
    """Определяет путь до узла, наращивая TTL (механизм traceroute)."""
    addr = resolve(host)
    ident = os.getpid() & 0xFFFF
    _active_ids.add(ident)
    sock = net.make_icmp_socket(timeout)
    print(f"traceroute до {host} ({addr}), максимум {max_hops} узлов:")
    try:
        for ttl in range(1, max_hops + 1):
            info = _trace_probe(sock, addr, ident, ttl, timeout)
            if info is None:
                print(f"{ttl:2d}  *")
                continue
            rtt = net.rtt_ms_from_payload(info["body"])
            print(f"{ttl:2d}  {info['ip']['src']}  {rtt:.2f} мс")
            if info["type"] == net.ICMP_ECHO_REPLY:
                print("Достигнут узел назначения.")
                return
    finally:
        _active_ids.discard(ident)
        sock.close()


def build_parser() -> argparse.ArgumentParser:
    """Разбор аргументов командной строки."""
    p = argparse.ArgumentParser(description="Параллельный ping и traceroute (ЛР5)")
    p.add_argument("hosts", nargs="+", help="один или несколько хостов")
    p.add_argument("--trace", action="store_true", help="режим traceroute для первого хоста")
    p.add_argument("--count", type=int, default=4, help="число запросов на хост")
    p.add_argument("--timeout", type=float, default=2.0, help="таймаут ответа, сек")
    p.add_argument("--max-hops", type=int, default=30, help="макс. число узлов traceroute")
    return p


def main(argv=None) -> int:
    """Точка входа."""
    args = build_parser().parse_args(argv)
    try:
        if args.trace:
            traceroute(args.hosts[0], args.max_hops, args.timeout)
        else:
            parallel_ping(args.hosts, args.count, args.timeout)
    except PermissionError:
        print("Нужны права root: запустите через sudo.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
