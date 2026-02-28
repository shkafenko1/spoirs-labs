import argparse
import socket
import os
import time
import select
import sys
from typing import Dict

import protocol

WINDOW_SIZE = 16
TIMEOUT = 0.5
MAX_RETRIES = 10

def _print_progress(bytes_transferred):
    sys.stdout.write(f"\rTransferred: {bytes_transferred / 1024:.1f} KB")
    sys.stdout.flush()

def calc_speed(bytes_cnt, seconds):
    if seconds <= 0: return "inf"
    mbps = (bytes_cnt * 8) / (seconds * 1_000_000)
    return f"{mbps:.2f} Mbps"

def upload_file(host: str, port: int, local_path: str, remote_name: str):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_addr = (host, port)
    
    try:
        f = open(local_path, "rb")
        file_size = os.path.getsize(local_path)
    except OSError:
        print("Cannot open local file")
        return

    print(f"Uploading {local_path} -> {remote_name} ({file_size} bytes)")
    
    sock.sendto(protocol.build_wrq(remote_name), server_addr)
    
    sock.settimeout(TIMEOUT)
    try:
        raw, addr = sock.recvfrom(1024)
        pkt = protocol.parse(raw)
        if pkt.opcode == protocol.ERROR:
            print(f"\nServer error: {pkt.error_msg}")
            return
        if pkt.opcode != protocol.ACK or pkt.block != 0:
            print("\nDid not receive ACK 0")
            return
    except socket.timeout:
        print("\nServer unreachable")
        return

    sock.setblocking(False)
    start_time = time.time()
    
    base = 1
    next_seq = 1
    window: Dict[int, bytes] = {}
    eof = False
    total_sent = 0
    timeouts = 0

    with f:
        while base <= next_seq or not eof:
            while next_seq < base + WINDOW_SIZE and not eof:
                chunk = f.read(protocol.MAX_DATA_LEN)
                if not chunk:
                    eof = True
                    break
                if len(chunk) < protocol.MAX_DATA_LEN:
                    eof = True
                
                pkt = protocol.build_data(next_seq, chunk)
                window[next_seq] = pkt
                sock.sendto(pkt, server_addr)
                total_sent += len(chunk)
                next_seq += 1
            
            _print_progress(total_sent)

            if eof and base == next_seq:
                break

            ready = select.select([sock], [], [], TIMEOUT)
            if ready[0]:
                try:
                    raw, _ = sock.recvfrom(1024)
                    ack = protocol.parse(raw)
                    if ack.opcode == protocol.ACK:
                        ack_num = ack.block
                        if ack_num >= (base & 0xFFFF):
                            shift = ack_num - (base & 0xFFFF) + 1
                            for _ in range(shift):
                                if base in window: del window[base]
                                base += 1
                            timeouts = 0
                except Exception: pass
            else:
                timeouts += 1
                if timeouts > MAX_RETRIES:
                    print("\nConnection timed out!")
                    return
                for i in range(base, next_seq):
                    if i in window:
                        sock.sendto(window[i], server_addr)

    duration = time.time() - start_time
    print(f"\nUpload Complete! Speed: {calc_speed(total_sent, duration)}")
    sock.close()

def download_file(host: str, port: int, remote_name: str, local_path: str):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_addr = (host, port)
    
    print(f"Downloading {remote_name} -> {local_path}")
    
    sock.sendto(protocol.build_rrq(remote_name), server_addr)
    
    try:
        f = open(local_path, "wb")
    except OSError:
        print("Cannot create local file")
        return

    start_time = time.time()
    expected = 1
    total_recv = 0
    retries = 0

    while True:
        ready = select.select([sock], [], [], TIMEOUT)
        if not ready[0]:
            retries += 1
            if retries > MAX_RETRIES:
                print("\nTimeout receiving data")
                break
            if expected == 1:
                sock.sendto(protocol.build_rrq(remote_name), server_addr)
            else:
                sock.sendto(protocol.build_ack((expected - 1) & 0xFFFF), server_addr)
            continue

        try:
            raw, addr = sock.recvfrom(4096)
            if addr != server_addr: continue
            pkt = protocol.parse(raw)
        except Exception: continue

        if pkt.opcode == protocol.ERROR:
            print(f"\nServer Error: {pkt.error_msg}")
            f.close()
            if os.path.exists(local_path):
                os.remove(local_path)
            return

        if pkt.opcode == protocol.DATA:
            if pkt.block == (expected & 0xFFFF):
                f.write(pkt.data)
                total_recv += len(pkt.data)
                sock.sendto(protocol.build_ack(expected), server_addr)
                expected += 1
                retries = 0
                _print_progress(total_recv)
                
                if len(pkt.data) < protocol.MAX_DATA_LEN:
                    break
            elif pkt.block == ((expected - 1) & 0xFFFF):
                sock.sendto(protocol.build_ack(pkt.block), server_addr)

    duration = time.time() - start_time
    print(f"\nDownload Complete! Speed: {calc_speed(total_recv, duration)}")
    sock.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=6969)
    sub = ap.add_subparsers(dest="cmd", required=True)
    
    up = sub.add_parser("upload")
    up.add_argument("local")
    up.add_argument("remote")
    
    dl = sub.add_parser("download")
    dl.add_argument("remote")
    dl.add_argument("local")
    
    args = ap.parse_args()
    
    if args.cmd == "upload":
        upload_file(args.host, args.port, args.local, args.remote)
    elif args.cmd == "download":
        download_file(args.host, args.port, args.remote, args.local)