import struct
from typing import Optional, Tuple

OP_RRQ = 1
OP_WRQ = 2
OP_DATA = 3
OP_ACK = 4
OP_ERROR = 5
OP_CHECKSUM = 6

ERR_FILE_NOT_FOUND = 1
ERR_FILE_EXISTS = 2
ERR_ACCESS_VIOLATION = 3
ERR_ILLEGAL_OPERATION = 4
ERR_UNKNOWN_TID = 5
ERR_CHECKSUM_MISMATCH = 6

ERROR_MESSAGES = {
    ERR_FILE_NOT_FOUND: "File not found",
    ERR_FILE_EXISTS: "File already exists",
    ERR_ACCESS_VIOLATION: "Access violation",
    ERR_ILLEGAL_OPERATION: "Illegal TFTP operation",
    ERR_UNKNOWN_TID: "Unknown transfer ID",
    ERR_CHECKSUM_MISMATCH: "Checksum mismatch",
}

BLOCK_SIZE = 512

_HDR = struct.Struct("!HH")


class ProtocolError(Exception):
    pass


def encode_zstr(s: str) -> bytes:
    return s.encode("utf-8") + b"\x00"


def decode_zstr(buf: bytes, offset: int = 0) -> Tuple[str, int]:
    end = buf.find(b"\x00", offset)
    if end == -1:
        raise ProtocolError("Missing zero terminator")
    return buf[offset:end].decode("utf-8", errors="strict"), end + 1


def pack_header(opcode: int, block: int) -> bytes:
    return _HDR.pack(opcode, block)


def unpack_header(packet: bytes) -> Tuple[int, int]:
    if len(packet) < _HDR.size:
        raise ProtocolError("Packet too short")
    return _HDR.unpack_from(packet, 0)


def parse_packet(packet: bytes) -> Tuple[int, int, bytes]:
    opcode, block = unpack_header(packet)
    return opcode, block, packet[_HDR.size :]


def build_rrq(filename: str, mode: str = "octet") -> bytes:
    return pack_header(OP_RRQ, 0) + encode_zstr(filename) + encode_zstr(mode)


def build_wrq(filename: str, mode: str = "octet") -> bytes:
    return pack_header(OP_WRQ, 0) + encode_zstr(filename) + encode_zstr(mode)


def parse_request(packet: bytes) -> Tuple[int, str, str]:
    opcode, block, payload = parse_packet(packet)
    if opcode not in (OP_RRQ, OP_WRQ) or block != 0:
        raise ProtocolError("Not a valid RRQ/WRQ")
    filename, off = decode_zstr(payload, 0)
    mode, off2 = decode_zstr(payload, off)
    if off2 != len(payload):
        raise ProtocolError("Trailing bytes in request")
    return opcode, filename, mode


def build_data(block: int, data: bytes) -> bytes:
    if not (1 <= block <= 0xFFFF):
        raise ProtocolError("Invalid block number")
    if len(data) > BLOCK_SIZE:
        raise ProtocolError("DATA payload too large")
    return pack_header(OP_DATA, block) + data


def parse_data(packet: bytes) -> Tuple[int, bytes]:
    opcode, block, payload = parse_packet(packet)
    if opcode != OP_DATA or block == 0:
        raise ProtocolError("Not a valid DATA")
    if len(payload) > BLOCK_SIZE:
        raise ProtocolError("DATA payload too large")
    return block, payload


def build_ack(block: int) -> bytes:
    if not (0 <= block <= 0xFFFF):
        raise ProtocolError("Invalid block number")
    return pack_header(OP_ACK, block)


def parse_ack(packet: bytes) -> int:
    opcode, block, payload = parse_packet(packet)
    if opcode != OP_ACK or payload:
        raise ProtocolError("Not a valid ACK")
    return block


def build_error(code: int, message: Optional[str] = None) -> bytes:
    if message is None:
        message = ERROR_MESSAGES.get(code, "Error")
    return pack_header(OP_ERROR, code) + encode_zstr(message)


def parse_error(packet: bytes) -> Tuple[int, str]:
    opcode, code, payload = parse_packet(packet)
    if opcode != OP_ERROR:
        raise ProtocolError("Not a valid ERROR")
    msg, off = decode_zstr(payload, 0)
    if off != len(payload):
        raise ProtocolError("Trailing bytes in ERROR")
    return code, msg


def build_checksum(digest32: bytes) -> bytes:
    if len(digest32) != 32:
        raise ProtocolError("Checksum must be 32 bytes")
    return pack_header(OP_CHECKSUM, 0) + digest32


def parse_checksum(packet: bytes) -> bytes:
    opcode, block, payload = parse_packet(packet)
    if opcode != OP_CHECKSUM or block != 0:
        raise ProtocolError("Not a valid CHECKSUM")
    if len(payload) != 32:
        raise ProtocolError("Invalid checksum payload length")
    return payload


def error_code_to_str(code: int) -> str:
    return ERROR_MESSAGES.get(code, f"Error {code}")
