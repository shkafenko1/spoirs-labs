import argparse
import os
import socket
import sys
from typing import Tuple

import protocol


DEFAULT_TIMEOUT = 3.0
DEFAULT_RETRIES = 5


def _stderr(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def _stdout(msg: str) -> None:
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def _recv(sock: socket.socket, timeout: float) -> Tuple[bytes, Tuple[str, int]]:
    sock.settimeout(timeout)
    return sock.recvfrom(4096)


def _expect_from(addr_expected: Tuple[str, int], addr_got: Tuple[str, int]) -> bool:
    return addr_expected == addr_got


def download(host: str, port: int, remote_filename: str, local_path: str, timeout: float, retries: int) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server = (host, port)

    rrq = protocol.build_rrq(remote_filename, "octet")
    _stdout(f"Downloading {remote_filename} -> {local_path}")

    last_sent = rrq
    expecting_block = 1

    try:
        with open(local_path, "wb") as out:
            attempts = 0
            sock.sendto(last_sent, server)

            while True:
                try:
                    raw, addr = _recv(sock, timeout)
                except socket.timeout:
                    attempts += 1
                    if attempts > retries:
                        _stderr("Error: timeout")
                        return 1
                    sock.sendto(last_sent, server)
                    continue

                if not _expect_from(server, addr):
                    continue

                attempts = 0

                try:
                    pkt = protocol.parse_packet(raw)
                except protocol.ProtocolError:
                    err = protocol.build_error(protocol.ERR_MALFORMED_PACKET, "Malformed packet")
                    sock.sendto(err, server)
                    _stderr("Error: malformed packet")
                    return 1

                if pkt.opcode == protocol.ERROR:
                    _stderr(f"ERROR {pkt.block}: {pkt.error_msg}")
                    return 1

                if pkt.opcode != protocol.DATA:
                    err = protocol.build_error(protocol.ERR_ILLEGAL_OPERATION, "Expected DATA")
                    sock.sendto(err, server)
                    _stderr("Error: illegal operation")
                    return 1

                if pkt.block == expecting_block:
                    data = pkt.data or b""
                    out.write(data)
                    ack = protocol.build_ack(expecting_block)
                    sock.sendto(ack, server)
                    last_sent = ack

                    if len(data) < protocol.MAX_DATA_LEN:
                        _stdout("Done")
                        return 0

                    expecting_block = (expecting_block + 1) & 0xFFFF
                    if expecting_block == 0:
                        expecting_block = 1
                elif pkt.block == ((expecting_block - 1) & 0xFFFF):
                    ack = protocol.build_ack(pkt.block)
                    sock.sendto(ack, server)
                    last_sent = ack
                else:
                    ack = protocol.build_ack((expecting_block - 1) & 0xFFFF)
                    sock.sendto(ack, server)
                    last_sent = ack

    except OSError as e:
        _stderr(f"Error: cannot write {local_path}: {e}")
        return 1
    finally:
        sock.close()


def upload(host: str, port: int, local_path: str, remote_filename: str, timeout: float, retries: int) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server = (host, port)

    try:
        f = open(local_path, "rb")
    except OSError as e:
        _stderr(f"Error: cannot read {local_path}: {e}")
        return 1

    _stdout(f"Uploading {local_path} -> {remote_filename}")

    with f:
        wrq = protocol.build_wrq(remote_filename, "octet")
        last_sent = wrq

        attempts = 0
        sock.sendto(last_sent, server)

        while True:
            try:
                raw, addr = _recv(sock, timeout)
            except socket.timeout:
                attempts += 1
                if attempts > retries:
                    _stderr("Error: timeout")
                    return 1
                sock.sendto(last_sent, server)
                continue

            if not _expect_from(server, addr):
                continue

            attempts = 0

            try:
                pkt = protocol.parse_packet(raw)
            except protocol.ProtocolError:
                err = protocol.build_error(protocol.ERR_MALFORMED_PACKET, "Malformed packet")
                sock.sendto(err, server)
                _stderr("Error: malformed packet")
                return 1

            if pkt.opcode == protocol.ERROR:
                _stderr(f"ERROR {pkt.block}: {pkt.error_msg}")
                return 1

            if pkt.opcode != protocol.ACK or pkt.block != 0:
                continue

            break

        block = 1
        while True:
            chunk = f.read(protocol.MAX_DATA_LEN)
            if chunk is None:
                chunk = b""

            data_pkt = protocol.build_data(block, chunk)
            last_sent = data_pkt
            sock.sendto(last_sent, server)

            attempts = 0
            while True:
                try:
                    raw, addr = _recv(sock, timeout)
                except socket.timeout:
                    attempts += 1
                    if attempts > retries:
                        _stderr("Error: timeout")
                        return 1
                    sock.sendto(last_sent, server)
                    continue

                if not _expect_from(server, addr):
                    continue

                try:
                    resp = protocol.parse_packet(raw)
                except protocol.ProtocolError:
                    continue

                if resp.opcode == protocol.ERROR:
                    _stderr(f"ERROR {resp.block}: {resp.error_msg}")
                    return 1

                if resp.opcode == protocol.ACK and resp.block == block:
                    break

            if len(chunk) < protocol.MAX_DATA_LEN:
                _stdout("Done")
                return 0

            block = (block + 1) & 0xFFFF
            if block == 0:
                block = 1

    sock.close()
    return 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", required=True, type=int)
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    ap.add_argument("--retries", type=int, default=DEFAULT_RETRIES)

    sub = ap.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("download")
    d.add_argument("remote_filename")
    d.add_argument("local_path")

    u = sub.add_parser("upload")
    u.add_argument("local_path")
    u.add_argument("remote_filename")

    args = ap.parse_args()

    if args.cmd == "download":
        raise SystemExit(download(args.host, args.port, args.remote_filename, args.local_path, args.timeout, args.retries))
    if args.cmd == "upload":
        raise SystemExit(upload(args.host, args.port, args.local_path, args.remote_filename, args.timeout, args.retries))


if __name__ == "__main__":
    main()
