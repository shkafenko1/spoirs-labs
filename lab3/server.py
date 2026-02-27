import argparse
import hashlib
import logging
import os
import random
import socket
import time
from pathlib import Path
from typing import Optional, Tuple

import protocol


LOG = logging.getLogger("tftp3.server")


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


def _recvfrom_checked(sock: socket.socket, expected_addr: Tuple[str, int]) -> bytes:
    pkt, addr = sock.recvfrom(2048)
    if addr != expected_addr:
        try:
            _sendto(sock, protocol.build_error(protocol.ERR_UNKNOWN_TID), addr, 0.0, 0.0)
        except Exception:
            pass
        raise protocol.ProtocolError("Unknown transfer ID")
    return pkt


def _send_error(sock: socket.socket, addr: Tuple[str, int], code: int, message: Optional[str] = None) -> None:
    pkt = protocol.build_error(code, message)
    try:
        sock.sendto(pkt, addr)
    except OSError:
        pass
    LOG.warning("sent ERROR to %s: code=%d msg=%s", addr, code, message or protocol.error_code_to_str(code))


def _safe_resolve(root: Path, filename: str) -> Path:
    p = (root / filename).resolve()
    if root.resolve() not in p.parents and p != root.resolve():
        raise PermissionError("path traversal")
    return p


def _wait_for_ack(
    sock: socket.socket,
    client_addr: Tuple[str, int],
    expected_block: int,
    timeout_s: float,
    max_retries: int,
    last_packet: bytes,
    stats: TransferStats,
    drop_rate: float,
    delay_s: float,
) -> bool:
    retransmits_for_block = 0
    while True:
        try:
            sock.settimeout(timeout_s)
            pkt = _recvfrom_checked(sock, client_addr)
        except socket.timeout:
            stats.timeouts += 1
            retransmits_for_block += 1
            stats.retransmissions += 1
            LOG.warning(
                "timeout waiting for ACK: client=%s block=%d retransmit=%d/%d",
                client_addr,
                expected_block,
                retransmits_for_block,
                max_retries,
            )
            if retransmits_for_block > max_retries:
                return False
            _sendto(sock, last_packet, client_addr, drop_rate, delay_s)
            continue
        except protocol.ProtocolError as e:
            LOG.debug("ignored packet from unknown TID: %s", e)
            continue

        try:
            opcode, block, _payload = protocol.parse_packet(pkt)
        except protocol.ProtocolError as e:
            LOG.debug("malformed packet while waiting ACK: %s", e)
            continue

        if opcode == protocol.OP_ERROR:
            try:
                code, msg = protocol.parse_error(pkt)
            except protocol.ProtocolError:
                code, msg = protocol.ERR_ILLEGAL_OPERATION, "Malformed ERROR"
            LOG.error("received ERROR from %s: code=%d msg=%s", client_addr, code, msg)
            return False

        if opcode != protocol.OP_ACK:
            LOG.debug("unexpected opcode while waiting ACK: op=%d", opcode)
            continue

        try:
            ack_block = protocol.parse_ack(pkt)
        except protocol.ProtocolError:
            LOG.debug("malformed ACK")
            continue

        if ack_block == expected_block:
            return True

        if ack_block < expected_block:
            LOG.debug("duplicate/old ACK ignored: got=%d expected=%d", ack_block, expected_block)
            continue

        LOG.warning("unexpected ACK block ignored: got=%d expected=%d", ack_block, expected_block)


def _send_checksum_and_wait_ack0(
    sock: socket.socket,
    client_addr: Tuple[str, int],
    digest32: bytes,
    timeout_s: float,
    max_retries: int,
    stats: TransferStats,
    drop_rate: float,
    delay_s: float,
) -> bool:
    pkt = protocol.build_checksum(digest32)
    tries = 0
    while True:
        _sendto(sock, pkt, client_addr, drop_rate, delay_s)
        try:
            sock.settimeout(timeout_s)
            resp = _recvfrom_checked(sock, client_addr)
        except socket.timeout:
            stats.timeouts += 1
            stats.retransmissions += 1
            tries += 1
            LOG.warning("timeout waiting for checksum ACK0: client=%s try=%d/%d", client_addr, tries, max_retries)
            if tries > max_retries:
                return False
            continue
        except protocol.ProtocolError:
            continue

        try:
            opcode, _block, _payload = protocol.parse_packet(resp)
        except protocol.ProtocolError:
            continue

        if opcode == protocol.OP_ACK:
            try:
                b = protocol.parse_ack(resp)
            except protocol.ProtocolError:
                continue
            if b == 0:
                return True
            LOG.debug("unexpected ACK after checksum: block=%d", b)
            continue

        if opcode == protocol.OP_ERROR:
            try:
                code, msg = protocol.parse_error(resp)
            except protocol.ProtocolError:
                code, msg = protocol.ERR_ILLEGAL_OPERATION, "Malformed ERROR"
            LOG.error("received ERROR after checksum from %s: code=%d msg=%s", client_addr, code, msg)
            return False


def handle_rrq(
    server_sock: socket.socket,
    client_addr: Tuple[str, int],
    root: Path,
    filename: str,
    timeout_s: float,
    max_retries: int,
    drop_rate: float,
    delay_s: float,
) -> None:
    stats = TransferStats(filename, "download")
    try:
        path = _safe_resolve(root, filename)
    except PermissionError:
        _send_error(server_sock, client_addr, protocol.ERR_ACCESS_VIOLATION)
        return

    if not path.exists() or not path.is_file():
        _send_error(server_sock, client_addr, protocol.ERR_FILE_NOT_FOUND)
        return

    try:
        f = path.open("rb")
    except PermissionError:
        _send_error(server_sock, client_addr, protocol.ERR_ACCESS_VIOLATION)
        return
    except OSError:
        _send_error(server_sock, client_addr, protocol.ERR_UNKNOWN_TID)
        return

    transfer_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    transfer_sock.bind(("0.0.0.0", 0))

    LOG.info("RRQ accepted: client=%s file=%s", client_addr, filename)

    h = hashlib.sha256()
    block = 1
    ok = True
    try:
        while True:
            data = f.read(protocol.BLOCK_SIZE)
            if data is None:
                data = b""
            h.update(data)

            pkt = protocol.build_data(block, data)
            _sendto(transfer_sock, pkt, client_addr, drop_rate, delay_s)

            if not _wait_for_ack(
                transfer_sock,
                client_addr,
                block,
                timeout_s,
                max_retries,
                pkt,
                stats,
                drop_rate,
                delay_s,
            ):
                ok = False
                _send_error(transfer_sock, client_addr, protocol.ERR_UNKNOWN_TID, "Transfer aborted (ACK timeout)")
                return

            stats.blocks = block
            stats.bytes += len(data)

            if len(data) < protocol.BLOCK_SIZE:
                break
            block = (block + 1) & 0xFFFF
            if block == 0:
                block = 1

        digest = h.digest()
        LOG.info("checksum (sender) file=%s sha256=%s", filename, digest.hex())
        if not _send_checksum_and_wait_ack0(
            transfer_sock, client_addr, digest, timeout_s, max_retries, stats, drop_rate, delay_s
        ):
            ok = False
            return
    finally:
        try:
            f.close()
        except Exception:
            pass
        try:
            transfer_sock.close()
        except Exception:
            pass
        if ok:
            stats.log_summary()


def _receive_checksum_and_ack_or_error(
    sock: socket.socket,
    client_addr: Tuple[str, int],
    expected_digest: bytes,
    timeout_s: float,
    max_retries: int,
    stats: TransferStats,
    drop_rate: float,
    delay_s: float,
) -> bool:
    tries = 0
    while True:
        try:
            sock.settimeout(timeout_s)
            pkt = _recvfrom_checked(sock, client_addr)
        except socket.timeout:
            stats.timeouts += 1
            tries += 1
            LOG.warning("timeout waiting for CHECKSUM: client=%s try=%d/%d", client_addr, tries, max_retries)
            if tries > max_retries:
                return False
            continue
        except protocol.ProtocolError:
            continue

        try:
            opcode, _block, _payload = protocol.parse_packet(pkt)
        except protocol.ProtocolError:
            continue

        if opcode == protocol.OP_CHECKSUM:
            try:
                got = protocol.parse_checksum(pkt)
            except protocol.ProtocolError:
                _send_error(sock, client_addr, protocol.ERR_ILLEGAL_OPERATION, "Malformed CHECKSUM")
                return False

            if got != expected_digest:
                LOG.error(
                    "checksum mismatch file=%s expected=%s got=%s",
                    stats.filename,
                    expected_digest.hex(),
                    got.hex(),
                )
                _send_error(sock, client_addr, protocol.ERR_CHECKSUM_MISMATCH)
                return False

            LOG.info("checksum verified file=%s sha256=%s", stats.filename, got.hex())
            _sendto(sock, protocol.build_ack(0), client_addr, drop_rate, delay_s)
            return True

        if opcode == protocol.OP_ERROR:
            try:
                code, msg = protocol.parse_error(pkt)
            except protocol.ProtocolError:
                code, msg = protocol.ERR_ILLEGAL_OPERATION, "Malformed ERROR"
            LOG.error("received ERROR while waiting checksum: code=%d msg=%s", code, msg)
            return False

        LOG.debug("ignoring packet while waiting checksum: op=%d", opcode)


def handle_wrq(
    server_sock: socket.socket,
    client_addr: Tuple[str, int],
    root: Path,
    filename: str,
    timeout_s: float,
    max_retries: int,
    overwrite: bool,
    drop_rate: float,
    delay_s: float,
) -> None:
    stats = TransferStats(filename, "upload")

    try:
        path = _safe_resolve(root, filename)
    except PermissionError:
        _send_error(server_sock, client_addr, protocol.ERR_ACCESS_VIOLATION)
        return

    if path.exists() and not overwrite:
        _send_error(server_sock, client_addr, protocol.ERR_FILE_EXISTS)
        return

    try:
        os.makedirs(path.parent, exist_ok=True)
    except OSError:
        _send_error(server_sock, client_addr, protocol.ERR_ACCESS_VIOLATION)
        return

    try:
        f = path.open("wb")
    except PermissionError:
        _send_error(server_sock, client_addr, protocol.ERR_ACCESS_VIOLATION)
        return
    except OSError:
        _send_error(server_sock, client_addr, protocol.ERR_UNKNOWN_TID)
        return

    transfer_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    transfer_sock.bind(("0.0.0.0", 0))

    LOG.info("WRQ accepted: client=%s file=%s", client_addr, filename)

    expected_block = 1
    last_acked = 0
    h = hashlib.sha256()
    ok = True

    try:
        ack0 = protocol.build_ack(0)
        _sendto(transfer_sock, ack0, client_addr, drop_rate, delay_s)

        retransmits_for_block = 0
        last_sent_ack = ack0

        while True:
            try:
                transfer_sock.settimeout(timeout_s)
                pkt = _recvfrom_checked(transfer_sock, client_addr)
            except socket.timeout:
                stats.timeouts += 1
                stats.retransmissions += 1
                retransmits_for_block += 1
                LOG.warning(
                    "timeout waiting DATA: client=%s expected_block=%d retransmit_ack=%d/%d",
                    client_addr,
                    expected_block,
                    retransmits_for_block,
                    max_retries,
                )
                if retransmits_for_block > max_retries:
                    ok = False
                    _send_error(transfer_sock, client_addr, protocol.ERR_UNKNOWN_TID, "Transfer aborted (DATA timeout)")
                    return
                _sendto(transfer_sock, last_sent_ack, client_addr, drop_rate, delay_s)
                continue
            except protocol.ProtocolError:
                continue

            retransmits_for_block = 0

            try:
                opcode, block, _payload = protocol.parse_packet(pkt)
            except protocol.ProtocolError:
                _send_error(transfer_sock, client_addr, protocol.ERR_ILLEGAL_OPERATION, "Malformed packet")
                ok = False
                return

            if opcode == protocol.OP_ERROR:
                try:
                    code, msg = protocol.parse_error(pkt)
                except protocol.ProtocolError:
                    code, msg = protocol.ERR_ILLEGAL_OPERATION, "Malformed ERROR"
                LOG.error("received ERROR from %s: code=%d msg=%s", client_addr, code, msg)
                ok = False
                return

            if opcode != protocol.OP_DATA:
                LOG.debug("unexpected opcode while receiving WRQ data: op=%d", opcode)
                continue

            try:
                data_block, data = protocol.parse_data(pkt)
            except protocol.ProtocolError:
                _send_error(transfer_sock, client_addr, protocol.ERR_ILLEGAL_OPERATION, "Malformed DATA")
                ok = False
                return

            if data_block == expected_block:
                f.write(data)
                h.update(data)

                stats.bytes += len(data)
                stats.blocks = expected_block

                last_acked = expected_block
                last_sent_ack = protocol.build_ack(last_acked)
                _sendto(transfer_sock, last_sent_ack, client_addr, drop_rate, delay_s)

                expected_block = (expected_block + 1) & 0xFFFF
                if expected_block == 0:
                    expected_block = 1

                if len(data) < protocol.BLOCK_SIZE:
                    break

            elif data_block == last_acked:
                LOG.debug("duplicate DATA received: block=%d (re-ACK)", data_block)
                _sendto(transfer_sock, protocol.build_ack(last_acked), client_addr, drop_rate, delay_s)

            elif data_block < expected_block:
                LOG.debug("old DATA received: got=%d expected=%d (re-ACK last=%d)", data_block, expected_block, last_acked)
                _sendto(transfer_sock, protocol.build_ack(last_acked), client_addr, drop_rate, delay_s)

            else:
                LOG.debug("out-of-order DATA ignored: got=%d expected=%d", data_block, expected_block)
                _sendto(transfer_sock, protocol.build_ack(last_acked), client_addr, drop_rate, delay_s)

        digest = h.digest()
        LOG.info("checksum (receiver) file=%s sha256=%s", filename, digest.hex())
        if not _receive_checksum_and_ack_or_error(
            transfer_sock,
            client_addr,
            digest,
            timeout_s,
            max_retries,
            stats,
            drop_rate,
            delay_s,
        ):
            ok = False
            return

    finally:
        try:
            f.close()
        except Exception:
            pass
        try:
            transfer_sock.close()
        except Exception:
            pass
        if ok:
            stats.log_summary()


def run_server(args: argparse.Namespace) -> None:
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.port))

    LOG.info("server started on %s:%d root=%s timeout=%.3f retries=%d", args.host, args.port, root, args.timeout, args.retries)

    try:
        while True:
            pkt, addr = sock.recvfrom(2048)
            try:
                opcode, filename, mode = protocol.parse_request(pkt)
            except protocol.ProtocolError as e:
                LOG.warning("invalid request from %s: %s", addr, e)
                _send_error(sock, addr, protocol.ERR_ILLEGAL_OPERATION, str(e))
                continue

            if mode.lower() != "octet":
                _send_error(sock, addr, protocol.ERR_ILLEGAL_OPERATION, "Only octet mode supported")
                continue

            if opcode == protocol.OP_RRQ:
                handle_rrq(sock, addr, root, filename, args.timeout, args.retries, args.drop, args.delay)
            elif opcode == protocol.OP_WRQ:
                handle_wrq(
                    sock,
                    addr,
                    root,
                    filename,
                    args.timeout,
                    args.retries,
                    args.overwrite,
                    args.drop,
                    args.delay,
                )
            else:
                _send_error(sock, addr, protocol.ERR_ILLEGAL_OPERATION, "Unsupported opcode")

    except KeyboardInterrupt:
        LOG.info("server stopping")
    finally:
        try:
            sock.close()
        except Exception:
            pass


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Lab3 UDP file transfer server (TFTP-like)")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--root", default="./storage")
    p.add_argument("--timeout", type=float, default=3.0)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--drop", type=float, default=0.0, help="debug: drop outgoing packets with given probability [0..1]")
    p.add_argument("--delay", type=float, default=0.0, help="debug: delay outgoing packets by seconds")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    run_server(args)


if __name__ == "__main__":
    main()
