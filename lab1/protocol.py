import struct
from dataclasses import dataclass
from typing import Tuple, Optional


RRQ = 1
WRQ = 2
DATA = 3
ACK = 4
ERROR = 5


ERR_UNDEFINED = 0
ERR_FILE_NOT_FOUND = 1
ERR_FILE_EXISTS = 2
ERR_ACCESS_VIOLATION = 3
ERR_ILLEGAL_OPERATION = 4
ERR_MALFORMED_PACKET = 5


MAX_DATA_LEN = 512


class ProtocolError(ValueError):
    pass


def _zterm_encode(s: str) -> bytes:
    b = s.encode("utf-8")
    if b"\x00" in b:
        raise ProtocolError("zero byte in string")
    return b + b"\x00"


def _zterm_decode(buf: bytes, start: int = 0) -> Tuple[str, int]:
    end = buf.find(b"\x00", start)
    if end == -1:
        raise ProtocolError("missing zero terminator")
    try:
        s = buf[start:end].decode("utf-8")
    except UnicodeDecodeError as e:
        raise ProtocolError("invalid utf-8") from e
    return s, end + 1


def build_rrq(filename: str, mode: str = "octet") -> bytes:
    return struct.pack("!HH", RRQ, 0) + _zterm_encode(filename) + _zterm_encode(mode)


def build_wrq(filename: str, mode: str = "octet") -> bytes:
    return struct.pack("!HH", WRQ, 0) + _zterm_encode(filename) + _zterm_encode(mode)


def build_data(block: int, payload: bytes) -> bytes:
    if not (1 <= block <= 0xFFFF):
        raise ProtocolError("invalid block")
    if len(payload) > MAX_DATA_LEN:
        raise ProtocolError("payload too large")
    return struct.pack("!HH", DATA, block) + payload


def build_ack(block: int) -> bytes:
    if not (0 <= block <= 0xFFFF):
        raise ProtocolError("invalid block")
    return struct.pack("!HH", ACK, block)


def build_error(code: int, message: str) -> bytes:
    if not (0 <= code <= 0xFFFF):
        raise ProtocolError("invalid error code")
    return struct.pack("!HH", ERROR, code) + _zterm_encode(message)


@dataclass(frozen=True)
class ParsedPacket:
    opcode: int
    block: int
    filename: Optional[str] = None
    mode: Optional[str] = None
    data: Optional[bytes] = None
    error_msg: Optional[str] = None


def parse_packet(pkt: bytes) -> ParsedPacket:
    if len(pkt) < 4:
        raise ProtocolError("packet too short")
    opcode, block = struct.unpack("!HH", pkt[:4])
    payload = pkt[4:]

    if opcode in (RRQ, WRQ):
        if block != 0:
            raise ProtocolError("rrq/wrq block must be 0")
        filename, off = _zterm_decode(payload, 0)
        mode, off2 = _zterm_decode(payload, off)
        if off2 != len(payload):
            raise ProtocolError("extra bytes in rrq/wrq")
        return ParsedPacket(opcode=opcode, block=block, filename=filename, mode=mode)

    if opcode == DATA:
        if block == 0:
            raise ProtocolError("data block must be >= 1")
        if len(payload) > MAX_DATA_LEN:
            raise ProtocolError("data payload too large")
        return ParsedPacket(opcode=opcode, block=block, data=payload)

    if opcode == ACK:
        if len(payload) != 0:
            raise ProtocolError("ack must have no payload")
        return ParsedPacket(opcode=opcode, block=block)

    if opcode == ERROR:
        msg, off = _zterm_decode(payload, 0) if payload else ("", 0)
        if payload and off != len(payload):
            raise ProtocolError("extra bytes in error")
        return ParsedPacket(opcode=opcode, block=block, error_msg=msg)

    raise ProtocolError("unknown opcode")
