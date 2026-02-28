import os
import socket
import time
from dataclasses import dataclass
from typing import Optional, Tuple


CRLF = b"\r\n"
LF = b"\n"


class ProtocolError(Exception):
    pass


@dataclass
class Command:
    name: str
    args: Tuple[str, ...]
    raw: str


def parse_command_line(line: str) -> Command:
    s = line.strip("\r\n")
    if not s:
        raise ProtocolError("empty command")

    parts = s.split()
    name = parts[0].upper()
    args = tuple(parts[1:])
    return Command(name=name, args=args, raw=s)


def format_ok(*parts: object) -> bytes:
    if parts:
        return ("OK " + " ".join(str(p) for p in parts) + "\n").encode("utf-8")
    return b"OK\n"


def format_err(message: str) -> bytes:
    return ("ERR " + message.replace("\n", " ") + "\n").encode("utf-8")


def set_tcp_keepalive(sock: socket.socket, *, idle: int = 30, interval: int = 10, count: int = 3) -> None:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    for opt_name, opt_val in (
        ("TCP_KEEPIDLE", idle),
        ("TCP_KEEPINTVL", interval),
        ("TCP_KEEPCNT", count),
    ):
        opt = getattr(socket, opt_name, None)
        if opt is None:
            continue
        try:
            sock.setsockopt(socket.IPPROTO_TCP, opt, opt_val)
        except OSError:
            pass


def monotonic() -> float:
    return time.monotonic()


def safe_filename(name: str) -> bool:
    if not name:
        return False
    if "\x00" in name:
        return False
    if "/" in name or "\\" in name:
        return False
    if name in (".", ".."):
        return False
    return True


def safe_join(root: str, filename: str) -> str:
    root_abs = os.path.abspath(root)
    path = os.path.abspath(os.path.join(root_abs, filename))
    if os.path.commonpath([root_abs, path]) != root_abs:
        raise PermissionError("path traversal")
    return path


def recv_some(sock: socket.socket, n: int) -> bytes:
    data = sock.recv(n)
    if data == b"":
        raise ConnectionError("connection closed")
    return data
