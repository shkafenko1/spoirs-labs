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
ERR_ACCESS_VIOLATION = 2
ERR_DISK_FULL = 3
ERR_ILLEGAL_OP = 4
ERR_FILE_EXISTS = 6

# Оптимальный размер payload для Ethernet MTU 1500
MAX_DATA_LEN = 1400

class ProtocolError(ValueError):
    pass

def _zterm_encode(s: str) -> bytes:
    return s.encode("utf-8") + b"\x00"

def _zterm_decode(buf: bytes, start: int = 0) -> Tuple[str, int]:
    end = buf.find(b"\x00", start)
    if end == -1:
        raise ProtocolError("missing zero terminator")
    return buf[start:end].decode("utf-8"), end + 1

def build_rrq(filename: str) -> bytes:
    return struct.pack("!H", RRQ) + _zterm_encode(filename)

def build_wrq(filename: str) -> bytes:
    return struct.pack("!H", WRQ) + _zterm_encode(filename)

def build_data(block: int, data: bytes) -> bytes:
    return struct.pack("!HH", DATA, block & 0xFFFF) + data

def build_ack(block: int) -> bytes:
    return struct.pack("!HH", ACK, block & 0xFFFF)

def build_error(code: int, msg: str) -> bytes:
    return struct.pack("!HH", ERROR, code) + _zterm_encode(msg)

@dataclass
class Packet:
    opcode: int
    block: int = 0
    filename: Optional[str] = None
    data: Optional[bytes] = None
    error_msg: Optional[str] = None

def parse(raw: bytes) -> Packet:
    if len(raw) < 2:
        raise ProtocolError("packet too short")
    opcode = struct.unpack("!H", raw[:2])[0]

    if opcode in (RRQ, WRQ):
        fname, _ = _zterm_decode(raw, 2)
        return Packet(opcode=opcode, filename=fname)
    
    elif opcode == DATA:
        if len(raw) < 4: raise ProtocolError("bad data packet")
        block = struct.unpack("!H", raw[2:4])[0]
        return Packet(opcode=opcode, block=block, data=raw[4:])
    
    elif opcode == ACK:
        if len(raw) < 4: raise ProtocolError("bad ack packet")
        block = struct.unpack("!H", raw[2:4])[0]
        return Packet(opcode=opcode, block=block)
    
    elif opcode == ERROR:
        if len(raw) < 4: raise ProtocolError("bad error packet")
        code = struct.unpack("!H", raw[2:4])[0]
        msg, _ = _zterm_decode(raw, 4)
        return Packet(opcode=opcode, block=code, error_msg=msg)
    
    raise ProtocolError(f"Unknown opcode {opcode}")