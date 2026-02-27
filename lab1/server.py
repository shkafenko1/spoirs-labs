import argparse
import logging
import os
import socket
import time
from typing import Optional, Tuple

import protocol


DEFAULT_TIMEOUT = 3.0
DEFAULT_RETRIES = 5


def _is_valid_filename(name: str) -> bool:
    if not name:
        return False
    if "/" in name or "\\" in name:
        return False
    if name in (".", ".."):
        return False
    if "\x00" in name:
        return False
    return True


def _safe_join(root: str, filename: str) -> str:
    root_abs = os.path.abspath(root)
    path = os.path.abspath(os.path.join(root_abs, filename))
    if os.path.commonpath([root_abs, path]) != root_abs:
        raise ValueError("path traversal")
    return path


def _recv_from(sock: socket.socket, timeout: float) -> Tuple[bytes, Tuple[str, int]]:
    sock.settimeout(timeout)
    data, addr = sock.recvfrom(4096)
    return data, addr


def _send(sock: socket.socket, addr: Tuple[str, int], pkt: bytes) -> None:
    sock.sendto(pkt, addr)


def _send_error(sock: socket.socket, addr: Tuple[str, int], code: int, msg: str) -> None:
    try:
        _send(sock, addr, protocol.build_error(code, msg))
    except Exception:
        logging.exception("failed to send error to %s", addr)


def handle_rrq(sock: socket.socket, client: Tuple[str, int], root: str, filename: str, timeout: float, retries: int) -> None:
    if not _is_valid_filename(filename):
        _send_error(sock, client, protocol.ERR_ILLEGAL_OPERATION, "Invalid file name")
        return

    path = _safe_join(root, filename)

    try:
        f = open(path, "rb")
    except FileNotFoundError:
        _send_error(sock, client, protocol.ERR_FILE_NOT_FOUND, "File not found")
        return
    except PermissionError:
        _send_error(sock, client, protocol.ERR_ACCESS_VIOLATION, "Access denied")
        return
    except OSError:
        _send_error(sock, client, protocol.ERR_UNDEFINED, "Cannot open file")
        return

    with f:
        block = 1
        while True:
            chunk = f.read(protocol.MAX_DATA_LEN)
            if chunk is None:
                chunk = b""

            pkt = protocol.build_data(block, chunk)
            attempts = 0

            while True:
                _send(sock, client, pkt)

                try:
                    resp_raw, resp_addr = _recv_from(sock, timeout)
                except socket.timeout:
                    attempts += 1
                    if attempts > retries:
                        logging.warning("RRQ %s: timeout waiting ACK block %d from %s", filename, block, client)
                        return
                    continue

                if resp_addr != client:
                    continue

                try:
                    resp = protocol.parse_packet(resp_raw)
                except protocol.ProtocolError:
                    _send_error(sock, client, protocol.ERR_MALFORMED_PACKET, "Malformed packet")
                    return

                if resp.opcode == protocol.ERROR:
                    logging.info("RRQ %s aborted by client %s: %d %s", filename, client, resp.block, resp.error_msg)
                    return

                if resp.opcode != protocol.ACK or resp.block != block:
                    continue

                break

            if len(chunk) < protocol.MAX_DATA_LEN:
                return

            block = (block + 1) & 0xFFFF
            if block == 0:
                block = 1


def handle_wrq(sock: socket.socket, client: Tuple[str, int], root: str, filename: str, allow_overwrite: bool, timeout: float, retries: int) -> None:
    if not _is_valid_filename(filename):
        _send_error(sock, client, protocol.ERR_ILLEGAL_OPERATION, "Invalid file name")
        return

    path = _safe_join(root, filename)

    if not allow_overwrite and os.path.exists(path):
        _send_error(sock, client, protocol.ERR_FILE_EXISTS, "File already exists")
        return

    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    except Exception:
        _send_error(sock, client, protocol.ERR_ACCESS_VIOLATION, "Cannot create directory")
        return

    try:
        f = open(path, "wb")
    except PermissionError:
        _send_error(sock, client, protocol.ERR_ACCESS_VIOLATION, "Access denied")
        return
    except OSError:
        _send_error(sock, client, protocol.ERR_UNDEFINED, "Cannot open file")
        return

    with f:
        ack0 = protocol.build_ack(0)
        _send(sock, client, ack0)

        expected = 1
        last_ack = ack0
        idle_timeouts = 0
        while True:
            try:
                raw, addr = _recv_from(sock, timeout)
            except socket.timeout:
                # retransmit last ack to prompt client
                idle_timeouts += 1
                if idle_timeouts > retries:
                    logging.warning("WRQ %s: timeout waiting DATA block %d from %s", filename, expected, client)
                    return
                _send(sock, client, last_ack)
                continue

            if addr != client:
                continue

            idle_timeouts = 0

            try:
                pkt = protocol.parse_packet(raw)
            except protocol.ProtocolError:
                _send_error(sock, client, protocol.ERR_MALFORMED_PACKET, "Malformed packet")
                return

            if pkt.opcode == protocol.ERROR:
                logging.info("WRQ %s aborted by client %s: %d %s", filename, client, pkt.block, pkt.error_msg)
                return

            if pkt.opcode == protocol.WRQ and pkt.block == 0:
                _send(sock, client, ack0)
                last_ack = ack0
                continue

            if pkt.opcode != protocol.DATA:
                _send_error(sock, client, protocol.ERR_ILLEGAL_OPERATION, "Expected DATA")
                return

            if pkt.block == expected:
                try:
                    f.write(pkt.data or b"")
                    f.flush()
                except OSError:
                    _send_error(sock, client, protocol.ERR_ACCESS_VIOLATION, "Write failed")
                    return

                ack = protocol.build_ack(expected)
                _send(sock, client, ack)
                last_ack = ack

                if len(pkt.data or b"") < protocol.MAX_DATA_LEN:
                    return

                expected = (expected + 1) & 0xFFFF
                if expected == 0:
                    expected = 1
            elif pkt.block == ((expected - 1) & 0xFFFF):
                # duplicate data, re-ack
                _send(sock, client, last_ack)
            else:
                # ignore out of order
                _send(sock, client, last_ack)


def serve_forever(host: str, port: int, root: str, allow_overwrite: bool, timeout: float, retries: int) -> None:
    os.makedirs(root, exist_ok=True)

    logging.info("server starting on %s:%d root=%s", host, port, os.path.abspath(root))

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind((host, port))

        while True:
            try:
                data, addr = sock.recvfrom(4096)
            except KeyboardInterrupt:
                break
            except Exception:
                logging.exception("recvfrom failed")
                continue

            try:
                pkt = protocol.parse_packet(data)
            except protocol.ProtocolError:
                logging.warning("malformed packet from %s", addr)
                _send_error(sock, addr, protocol.ERR_MALFORMED_PACKET, "Malformed packet")
                continue

            if pkt.opcode == protocol.RRQ:
                logging.info("request from %s RRQ %s", addr, pkt.filename)
                try:
                    handle_rrq(sock, addr, root, pkt.filename or "", timeout, retries)
                except Exception:
                    logging.exception("RRQ handler failed for %s", addr)
                    _send_error(sock, addr, protocol.ERR_UNDEFINED, "Server error")
            elif pkt.opcode == protocol.WRQ:
                logging.info("request from %s WRQ %s", addr, pkt.filename)
                try:
                    handle_wrq(sock, addr, root, pkt.filename or "", allow_overwrite, timeout, retries)
                except Exception:
                    logging.exception("WRQ handler failed for %s", addr)
                    _send_error(sock, addr, protocol.ERR_UNDEFINED, "Server error")
            else:
                _send_error(sock, addr, protocol.ERR_ILLEGAL_OPERATION, "Illegal operation")

    finally:
        try:
            sock.close()
        except Exception:
            pass
        logging.info("server stopped")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", required=True, type=int)
    ap.add_argument("--root", required=True)
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    ap.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    ap.add_argument("--allow-overwrite", action="store_true", default=False)
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    serve_forever(args.host, args.port, args.root, args.allow_overwrite, args.timeout, args.retries)


if __name__ == "__main__":
    main()
