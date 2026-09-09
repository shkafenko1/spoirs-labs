"""Низкоуровневые помощники для работы с ICMP и разбора IP-заголовка.

Используются в parallel-ping (ping.py) и в демонстрации smurf-атаки (smurf.py).
Кроссплатформенно: только стандартная библиотека (socket, struct, os, time).
"""
import os
import socket
import struct
import time

ICMP_ECHO_REQUEST = 8
ICMP_ECHO_REPLY = 0
ICMP_TIME_EXCEEDED = 11
ICMP_DEST_UNREACH = 3

# В теле echo-пакета передаём метку времени (double) -> RTT считаем по ней.
_PAYLOAD_FMT = "!d"
_PAYLOAD_SIZE = struct.calcsize(_PAYLOAD_FMT)


def checksum(data: bytes) -> int:
    """Стандартная контрольная сумма ICMP (RFC 1071)."""
    if len(data) % 2:
        data += b"\x00"
    total = 0
    for i in range(0, len(data), 2):
        total += (data[i] << 8) + data[i + 1]
    total = (total >> 16) + (total & 0xFFFF)
    total += total >> 16
    return (~total) & 0xFFFF


def build_echo_request(ident: int, seq: int) -> bytes:
    """Собирает ICMP echo-request с меткой времени в теле."""
    payload = struct.pack(_PAYLOAD_FMT, time.time())
    header = struct.pack("!BBHHH", ICMP_ECHO_REQUEST, 0, 0, ident & 0xFFFF, seq & 0xFFFF)
    chk = checksum(header + payload)
    header = struct.pack("!BBHHH", ICMP_ECHO_REQUEST, 0, chk, ident & 0xFFFF, seq & 0xFFFF)
    return header + payload


def parse_ip_header(packet: bytes) -> dict:
    """Разбирает IPv4-заголовок, возвращает поля и смещение начала данных."""
    ihl = (packet[0] & 0x0F) * 4
    fields = struct.unpack("!BBHHHBBH4s4s", packet[:20])
    return {
        "ihl": ihl,
        "ttl": fields[5],
        "proto": fields[6],
        "src": socket.inet_ntoa(fields[8]),
        "dst": socket.inet_ntoa(fields[9]),
    }


def parse_icmp(packet: bytes) -> dict:
    """Разбирает ICMP-сообщение внутри полученного IP-пакета."""
    ip = parse_ip_header(packet)
    off = ip["ihl"]
    icmp_type, code, _chk, ident, seq = struct.unpack("!BBHHH", packet[off:off + 8])
    body = packet[off + 8:]
    return {"ip": ip, "type": icmp_type, "code": code, "id": ident, "seq": seq, "body": body}


def original_ident_from_error(body: bytes) -> int:
    """Для сообщений об ошибке (TTL exceeded / unreachable) достаёт ident.

    В теле ICMP-ошибки лежит IP-заголовок + первые 8 байт исходного пакета.
    """
    if len(body) < 20:
        return -1
    inner_ihl = (body[0] & 0x0F) * 4
    if len(body) < inner_ihl + 8:
        return -1
    _t, _c, _chk, ident, _seq = struct.unpack("!BBHHH", body[inner_ihl:inner_ihl + 8])
    return ident


def rtt_ms_from_payload(body: bytes) -> float:
    """Вычисляет RTT (мс) по метке времени в теле echo-reply."""
    if len(body) < _PAYLOAD_SIZE:
        return -1.0
    (sent,) = struct.unpack(_PAYLOAD_FMT, body[:_PAYLOAD_SIZE])
    return (time.time() - sent) * 1000.0


def make_icmp_socket(timeout: float = 1.0) -> socket.socket:
    """Создаёт raw ICMP-сокет (нужны права root/administrator)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
    sock.settimeout(timeout)
    return sock
