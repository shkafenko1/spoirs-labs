import argparse
import socket
import os
import time
import select
import sys
import struct
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

# --- UDP CLIENT ---

def udp_upload(host: str, port: int, local_path: str, remote_name: str):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server = (host, port)
    try:
        f = open(local_path, "rb")
        file_size = os.path.getsize(local_path)
    except OSError:
        print("Error reading file")
        return

    print(f"[UDP] Uploading {local_path} ({file_size} bytes)")
    sock.sendto(protocol.build_wrq(remote_name), server)
    
    sock.settimeout(TIMEOUT)
    try:
        raw, _ = sock.recvfrom(1024)
        if protocol.parse(raw).opcode != protocol.ACK: return
    except socket.timeout:
        print("Server unreachable")
        return

    sock.setblocking(False)
    start_time = time.time()
    base, next_seq = 1, 1
    window = {}
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
                if len(chunk) < protocol.MAX_DATA_LEN: eof = True
                pkt = protocol.build_data(next_seq, chunk)
                window[next_seq] = pkt
                sock.sendto(pkt, server)
                total_sent += len(chunk)
                next_seq += 1
            
            _print_progress(total_sent)
            if eof and base == next_seq: break

            ready = select.select([sock], [], [], TIMEOUT)
            if ready[0]:
                try:
                    raw, _ = sock.recvfrom(1024)
                    ack = protocol.parse(raw)
                    if ack.opcode == protocol.ACK:
                        if ack.block >= (base & 0xFFFF):
                            shift = ack.block - (base & 0xFFFF) + 1
                            for _ in range(shift):
                                if base in window: del window[base]
                                base += 1
                            timeouts = 0
                except: pass
            else:
                timeouts += 1
                if timeouts > MAX_RETRIES: return
                for i in range(base, next_seq):
                    if i in window: sock.sendto(window[i], server)

    print(f"\n[UDP] Done! Speed: {calc_speed(total_sent, time.time() - start_time)}")
    sock.close()

def udp_download(host: str, port: int, remote_name: str, local_path: str):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server = (host, port)
    print(f"[UDP] Downloading {remote_name}")
    sock.sendto(protocol.build_rrq(remote_name), server)
    
    try:
        f = open(local_path, "wb")
    except OSError: return

    start_time = time.time()
    expected = 1
    total_recv = 0
    retries = 0

    while True:
        ready = select.select([sock], [], [], TIMEOUT)
        if not ready[0]:
            retries += 1
            if retries > MAX_RETRIES: break
            if expected == 1: sock.sendto(protocol.build_rrq(remote_name), server)
            else: sock.sendto(protocol.build_ack((expected - 1) & 0xFFFF), server)
            continue

        try:
            raw, addr = sock.recvfrom(4096)
            if addr != server: continue
            pkt = protocol.parse(raw)
        except: continue

        if pkt.opcode == protocol.ERROR:
            print(f"Error: {pkt.error_msg}")
            f.close()
            os.remove(local_path)
            return

        if pkt.opcode == protocol.DATA:
            if pkt.block == (expected & 0xFFFF):
                f.write(pkt.data)
                total_recv += len(pkt.data)
                sock.sendto(protocol.build_ack(expected), server)
                expected += 1
                retries = 0
                _print_progress(total_recv)
                if len(pkt.data) < protocol.MAX_DATA_LEN: break
            elif pkt.block == ((expected - 1) & 0xFFFF):
                sock.sendto(protocol.build_ack(pkt.block), server)

    print(f"\n[UDP] Done! Speed: {calc_speed(total_recv, time.time() - start_time)}")
    sock.close()

# --- TCP CLIENT ---

def tcp_upload(host: str, port: int, local_path: str, remote_name: str):
    try:
        f = open(local_path, "rb")
        file_size = os.path.getsize(local_path)
    except OSError:
        print("File not found")
        return

    print(f"[TCP] Uploading {local_path} ({file_size} bytes)")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        # Send WRQ Opcode + Name Len + Name
        encoded_name = remote_name.encode('utf-8')
        header = struct.pack("!BH", protocol.WRQ, len(encoded_name)) + encoded_name
        sock.sendall(header)
        
        # Wait for OK
        resp = sock.recv(1)
        if resp != b'\x01':
            print("Server rejected upload")
            return

        start_time = time.time()
        total_sent = 0
        while True:
            chunk = f.read(16384)
            if not chunk: break
            sock.sendall(chunk)
            total_sent += len(chunk)
            _print_progress(total_sent)
            
        print(f"\n[TCP] Done! Speed: {calc_speed(total_sent, time.time() - start_time)}")
    finally:
        sock.close()
        f.close()

def tcp_download(host: str, port: int, remote_name: str, local_path: str):
    print(f"[TCP] Downloading {remote_name}")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        # Send RRQ Opcode + Name Len + Name
        encoded_name = remote_name.encode('utf-8')
        header = struct.pack("!BH", protocol.RRQ, len(encoded_name)) + encoded_name
        sock.sendall(header)
        
        # Wait for OK
        resp = sock.recv(1)
        if resp != b'\x01':
            print("Server rejected download (File not found?)")
            return
            
        f = open(local_path, "wb")
        start_time = time.time()
        total_recv = 0
        while True:
            chunk = sock.recv(16384)
            if not chunk: break
            f.write(chunk)
            total_recv += len(chunk)
            _print_progress(total_recv)
            
        print(f"\n[TCP] Done! Speed: {calc_speed(total_recv, time.time() - start_time)}")
    finally:
        sock.close()
        if 'f' in locals(): f.close()

# --- MAIN ---

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=6969)
    ap.add_argument("--protocol", choices=["tcp", "udp"], default="udp")
    sub = ap.add_subparsers(dest="cmd", required=True)
    
    up = sub.add_parser("upload")
    up.add_argument("local")
    up.add_argument("remote")
    
    dl = sub.add_parser("download")
    dl.add_argument("remote")
    dl.add_argument("local")
    
    args = ap.parse_args()
    
    if args.protocol == "udp":
        if args.cmd == "upload": udp_upload(args.host, args.port, args.local, args.remote)
        elif args.cmd == "download": udp_download(args.host, args.port, args.remote, args.local)
    else:
        if args.cmd == "upload": tcp_upload(args.host, args.port, args.local, args.remote)
        elif args.cmd == "download": tcp_download(args.host, args.port, args.remote, args.local)