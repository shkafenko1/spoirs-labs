import argparse
import hashlib
import logging
import os
import random
import socket
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

import protocol


LOG = logging.getLogger("tftp3.client")


class TransferStats:
    def __init__(self, filename: str, direction: str):
        self.filename = filename
        self.direction = direction
        self.bytes = 0
        self.blocks = 0
        self.timeouts = 0
        self.retransmissions = 0

    def log_summary(self) -> None:
        LOG.info(
            "transfer summary: file=%s direction=%s bytes=%d blocks=%d timeouts=%d retransmissions=%d",
            self.filename,
            self.direction,
            self.bytes,
            self.blocks,
            self.timeouts,
            self.retransmissions,
        )


def _maybe_debug_drop_or_delay(packet: bytes, drop_rate: float, delay_s: float) -> Optional[bytes]:
    if drop_rate > 0.0 and random.random() < drop_rate:
        return None
    if delay_s > 0.0:
        time.sleep(delay_s)
    return packet


def _sendto(sock: socket.socket, packet: bytes, addr: Tuple[str, int], drop_rate: float, delay_s: float) -> None:
    pkt = _maybe_debug_drop_or_delay(packet, drop_rate, delay_s)
    if pkt is None:
        LOG.debug("debug drop: len=%d to=%s", len(packet), addr)
        return
    sock.sendto(pkt, addr)


def _recvfrom(sock: socket.socket) -> Tuple[bytes, Tuple[str, int]]:
    return sock.recvfrom(2048)


def _stderr_error(code: int, msg: str) -> None:
    sys.stderr.write(f"ERROR {code}: {msg}\n")


def _send_error(sock: socket.socket, addr: Tuple[str, int], code: int, message: Optional[str] = None) -> None:
    pkt = protocol.build_error(code, message)
    try:
        sock.sendto(pkt, addr)
    except OSError:
        pass


def download(
    host: str,
    port: int,
    timeout_s: float,
    max_retries: int,
    remote_filename: str,
    local_path: Path,
    drop_rate: float,
    delay_s: float,
) -> int:
    stats = TransferStats(remote_filename, "download")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 0))

    server_addr = (host, port)
    server_tid: Optional[Tuple[str, int]] = None

    req = protocol.build_rrq(remote_filename)

    try:
        f = local_path.open("wb")
    except OSError as e:
        _stderr_error(protocol.ERR_ACCESS_VIOLATION, str(e))
        return 2

    h = hashlib.sha256()

    expected_block = 1
    last_acked = 0
    last_ack_pkt = protocol.build_ack(0)

    try:
        _sendto(sock, req, server_addr, drop_rate, delay_s)

        retries = 0
        while True:
            try:
                sock.settimeout(timeout_s)
                pkt, addr = _recvfrom(sock)
            except socket.timeout:
                stats.timeouts += 1
                stats.retransmissions += 1
                retries += 1
                LOG.warning("timeout waiting DATA/CHECKSUM: expected_block=%d retry=%d/%d", expected_block, retries, max_retries)
                if retries > max_retries:
                    _stderr_error(protocol.ERR_UNKNOWN_TID, "Transfer aborted (timeout)")
                    return 1
                if expected_block == 1:
                    _sendto(sock, req, server_addr, drop_rate, delay_s)
                else:
                    _sendto(sock, last_ack_pkt, server_tid or server_addr, drop_rate, delay_s)
                continue

            if server_tid is None:
                server_tid = addr
            elif addr != server_tid:
                _send_error(sock, addr, protocol.ERR_UNKNOWN_TID)
                continue

            retries = 0

            try:
                opcode, block, payload = protocol.parse_packet(pkt)
            except protocol.ProtocolError:
                continue

            if opcode == protocol.OP_ERROR:
                try:
                    code, msg = protocol.parse_error(pkt)
                except protocol.ProtocolError:
                    code, msg = protocol.ERR_ILLEGAL_OPERATION, "Malformed ERROR"
                _stderr_error(code, msg)
                return 1

            if opcode == protocol.OP_DATA:
                try:
                    bno, data = protocol.parse_data(pkt)
                except protocol.ProtocolError:
                    _sendto(sock, protocol.build_error(protocol.ERR_ILLEGAL_OPERATION, "Malformed DATA"), server_tid, drop_rate, delay_s)
                    return 1

                if bno == expected_block:
                    f.write(data)
                    h.update(data)

                    stats.bytes += len(data)
                    stats.blocks = expected_block

                    last_acked = expected_block
                    last_ack_pkt = protocol.build_ack(last_acked)
                    _sendto(sock, last_ack_pkt, server_tid, drop_rate, delay_s)

                    expected_block = (expected_block + 1) & 0xFFFF
                    if expected_block == 0:
                        expected_block = 1

                    if len(data) < protocol.BLOCK_SIZE:
                        continue

                elif bno == last_acked:
                    LOG.debug("duplicate DATA received: block=%d (re-ACK)", bno)
                    _sendto(sock, protocol.build_ack(last_acked), server_tid, drop_rate, delay_s)
                elif bno < expected_block:
                    LOG.debug("old DATA received: got=%d expected=%d (re-ACK last=%d)", bno, expected_block, last_acked)
                    _sendto(sock, protocol.build_ack(last_acked), server_tid, drop_rate, delay_s)
                else:
                    LOG.debug("out-of-order DATA ignored: got=%d expected=%d", bno, expected_block)
                    _sendto(sock, protocol.build_ack(last_acked), server_tid, drop_rate, delay_s)

                continue

            if opcode == protocol.OP_CHECKSUM:
                try:
                    sender_digest = protocol.parse_checksum(pkt)
                except protocol.ProtocolError:
                    _sendto(sock, protocol.build_error(protocol.ERR_ILLEGAL_OPERATION, "Malformed CHECKSUM"), server_tid, drop_rate, delay_s)
                    return 1

                recv_digest = h.digest()
                LOG.info("checksum (receiver) file=%s sha256=%s", remote_filename, recv_digest.hex())
                LOG.info("checksum (sender)   file=%s sha256=%s", remote_filename, sender_digest.hex())

                if sender_digest != recv_digest:
                    _stderr_error(protocol.ERR_CHECKSUM_MISMATCH, protocol.error_code_to_str(protocol.ERR_CHECKSUM_MISMATCH))
                    _sendto(sock, protocol.build_error(protocol.ERR_CHECKSUM_MISMATCH), server_tid, drop_rate, delay_s)
                    return 1

                _sendto(sock, protocol.build_ack(0), server_tid, drop_rate, delay_s)
                stats.log_summary()
                return 0

    finally:
        try:
            f.close()
        except Exception:
            pass
        try:
            sock.close()
        except Exception:
            pass


def upload(
    host: str,
    port: int,
    timeout_s: float,
    max_retries: int,
    local_path: Path,
    remote_filename: str,
    drop_rate: float,
    delay_s: float,
) -> int:
    stats = TransferStats(remote_filename, "upload")

    try:
        f = local_path.open("rb")
    except OSError as e:
        _stderr_error(protocol.ERR_FILE_NOT_FOUND, str(e))
        return 2

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 0))

    server_addr = (host, port)
    server_tid: Optional[Tuple[str, int]] = None

    req = protocol.build_wrq(remote_filename)

    h = hashlib.sha256()

    try:
        _sendto(sock, req, server_addr, drop_rate, delay_s)

        retries = 0
        while True:
            try:
                sock.settimeout(timeout_s)
                pkt, addr = _recvfrom(sock)
            except socket.timeout:
                stats.timeouts += 1
                stats.retransmissions += 1
                retries += 1
                LOG.warning("timeout waiting ACK0: retry=%d/%d", retries, max_retries)
                if retries > max_retries:
                    _stderr_error(protocol.ERR_UNKNOWN_TID, "Transfer aborted (ACK0 timeout)")
                    return 1
                _sendto(sock, req, server_addr, drop_rate, delay_s)
                continue

            if server_tid is None:
                server_tid = addr
            elif addr != server_tid:
                _send_error(sock, addr, protocol.ERR_UNKNOWN_TID)
                continue

            try:
                opcode, _block, _payload = protocol.parse_packet(pkt)
            except protocol.ProtocolError:
                continue

            if opcode == protocol.OP_ERROR:
                try:
                    code, msg = protocol.parse_error(pkt)
                except protocol.ProtocolError:
                    code, msg = protocol.ERR_ILLEGAL_OPERATION, "Malformed ERROR"
                _stderr_error(code, msg)
                return 1

            if opcode != protocol.OP_ACK:
                continue

            try:
                b = protocol.parse_ack(pkt)
            except protocol.ProtocolError:
                continue

            if b == 0:
                break

        block = 1
        last_packet: Optional[bytes] = None

        while True:
            data = f.read(protocol.BLOCK_SIZE)
            if data is None:
                data = b""
            h.update(data)

            pkt = protocol.build_data(block, data)
            last_packet = pkt
            _sendto(sock, pkt, server_tid, drop_rate, delay_s)

            retransmits_for_block = 0
            while True:
                try:
                    sock.settimeout(timeout_s)
                    resp, addr = _recvfrom(sock)
                except socket.timeout:
                    stats.timeouts += 1
                    stats.retransmissions += 1
                    retransmits_for_block += 1
                    LOG.warning(
                        "timeout waiting ACK: block=%d retransmit=%d/%d",
                        block,
                        retransmits_for_block,
                        max_retries,
                    )
                    if retransmits_for_block > max_retries:
                        _stderr_error(protocol.ERR_UNKNOWN_TID, "Transfer aborted (ACK timeout)")
                        return 1
                    _sendto(sock, last_packet, server_tid, drop_rate, delay_s)
                    continue

                if addr != server_tid:
                    _send_error(sock, addr, protocol.ERR_UNKNOWN_TID)
                    continue

                try:
                    opcode, _b2, _pl2 = protocol.parse_packet(resp)
                except protocol.ProtocolError:
                    continue

                if opcode == protocol.OP_ERROR:
                    try:
                        code, msg = protocol.parse_error(resp)
                    except protocol.ProtocolError:
                        code, msg = protocol.ERR_ILLEGAL_OPERATION, "Malformed ERROR"
                    _stderr_error(code, msg)
                    return 1

                if opcode != protocol.OP_ACK:
                    continue

                try:
                    ack_block = protocol.parse_ack(resp)
                except protocol.ProtocolError:
                    continue

                if ack_block == block:
                    break

                if ack_block < block:
                    LOG.debug("duplicate/old ACK ignored: got=%d expected=%d", ack_block, block)
                    continue

            stats.blocks = block
            stats.bytes += len(data)

            if len(data) < protocol.BLOCK_SIZE:
                break

            block = (block + 1) & 0xFFFF
            if block == 0:
                block = 1

        digest = h.digest()
        LOG.info("checksum (sender) file=%s sha256=%s", remote_filename, digest.hex())

        checksum_pkt = protocol.build_checksum(digest)
        tries = 0
        while True:
            _sendto(sock, checksum_pkt, server_tid, drop_rate, delay_s)
            try:
                sock.settimeout(timeout_s)
                resp, addr = _recvfrom(sock)
            except socket.timeout:
                stats.timeouts += 1
                stats.retransmissions += 1
                tries += 1
                LOG.warning("timeout waiting checksum ACK0: try=%d/%d", tries, max_retries)
                if tries > max_retries:
                    _stderr_error(protocol.ERR_UNKNOWN_TID, "Transfer aborted (checksum timeout)")
                    return 1
                continue

            if addr != server_tid:
                _send_error(sock, addr, protocol.ERR_UNKNOWN_TID)
                continue

            try:
                opcode, _b, _p = protocol.parse_packet(resp)
            except protocol.ProtocolError:
                continue

            if opcode == protocol.OP_ACK:
                try:
                    b0 = protocol.parse_ack(resp)
                except protocol.ProtocolError:
                    continue
                if b0 == 0:
                    stats.log_summary()
                    return 0
                continue

            if opcode == protocol.OP_ERROR:
                try:
                    code, msg = protocol.parse_error(resp)
                except protocol.ProtocolError:
                    code, msg = protocol.ERR_ILLEGAL_OPERATION, "Malformed ERROR"
                _stderr_error(code, msg)
                return 1

    finally:
        try:
            f.close()
        except Exception:
            pass
        try:
            sock.close()
        except Exception:
            pass


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Lab3 UDP file transfer client (TFTP-like)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--timeout", type=float, default=3.0)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--drop", type=float, default=0.0, help="debug: drop outgoing packets with given probability [0..1]")
    p.add_argument("--delay", type=float, default=0.0, help="debug: delay outgoing packets by seconds")

    sub = p.add_subparsers(dest="command", required=True)

    d = sub.add_parser("download")
    d.add_argument("remote_filename")
    d.add_argument("local_path")

    u = sub.add_parser("upload")
    u.add_argument("local_path")
    u.add_argument("remote_filename")

    return p


def main() -> None:
    args = build_arg_parser().parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.command == "download":
        rc = download(
            args.host,
            args.port,
            args.timeout,
            args.retries,
            args.remote_filename,
            Path(args.local_path),
            args.drop,
            args.delay,
        )
        raise SystemExit(rc)

    if args.command == "upload":
        rc = upload(
            args.host,
            args.port,
            args.timeout,
            args.retries,
            Path(args.local_path),
            args.remote_filename,
            args.drop,
            args.delay,
        )
        raise SystemExit(rc)


if __name__ == "__main__":
    main()
