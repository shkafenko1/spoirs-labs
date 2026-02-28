import argparse
import socket
import os
import time
import select
import sys
import struct
from typing import Dict

import protocol

WINDOW_SIZE = 64
TIMEOUT = 0.1
MAX_RETRIES = 20
SOCKET_BUF_SIZE = 1 * 1024 * 1024

def configure_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUF_SIZE)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUF_SIZE)
    except: pass

def _print_progress(bytes_transferred):
    if bytes_transferred % (512 * 1024) < 2000: 
        sys.stdout.write(f"\rTransferred: {bytes_transferred / 1024 / 1024:.2f} MB")
        sys.stdout.flush()

def calc_speed(bytes_cnt, seconds):
    if seconds <= 0: return "inf"
    mbps = (bytes_cnt * 8) / (seconds * 1_000_000)
    return f"{mbps:.2f} Mbps"

# --- UDP CLIENT ---

def udp_upload(host: str, port: int, local_path: str, remote_name: str):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server = (host, port)
    configure_socket(sock)
    
    try:
        f = open(local_path, "rb")
        file_size = os.path.getsize(local_path)
    except OSError:
        print("Error reading file")
        return

    print(f"[UDP] Uploading {local_path} ({file_size} bytes)")
    sock.sendto(protocol.build_wrq(remote_name), server)
    
    # Wait for ACK 0
    sock.settimeout(2.0)
    try:
        data, _ = sock.recvfrom(1024)
        if len(data) >= 4:
            opcode = struct.unpack("!H", data[:2])[0]
            if opcode != protocol.ACK:
                print("Bad handshake")
                return
    except socket.timeout:
        print("Server unreachable")
        return
    sock.settimeout(None)

    start_time = time.time()
    
    base = 1
    next_seq = 1
    window_buff: Dict[int, bytes] = {}
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
                window_buff[next_seq] = pkt
                sock.sendto(pkt, server)
                total_sent += len(chunk)
                next_seq += 1
            
            _print_progress(total_sent)

            ready = select.select([sock], [], [], TIMEOUT)
            if ready[0]:
                try:
                    raw, _ = sock.recvfrom(1024)
                    if len(raw) >= 4:
                        opcode = struct.unpack("!H", raw[:2])[0]
                        if opcode == protocol.ACK:
                            ack_val = struct.unpack("!H", raw[2:4])[0]
                            if ack_val >= (base & 0xFFFF):
                                shift = ack_val - (base & 0xFFFF) + 1
                                for _ in range(shift):
                                    if base in window_buff: del window_buff[base]
                                    base += 1
                                timeouts = 0
                except: pass
            else:
                timeouts += 1
                if timeouts > MAX_RETRIES:
                    print("\nConnection timed out!")
                    return
                for seq in range(base, next_seq):
                    if seq in window_buff:
                        sock.sendto(window_buff[seq], server)

    print(f"\n[UDP] Done! Speed: {calc_speed(total_sent, time.time() - start_time)}")
    sock.close()

def udp_download(host: str, port: int, remote_name: str, local_path: str):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server = (host, port)
    configure_socket(sock)
    
    print(f"[UDP] Downloading {remote_name}")
    sock.sendto(protocol.build_rrq(remote_name), server)
    
    try:
        f = open(local_path, "wb")
    except OSError: return

    start_time = time.time()
    expected = 1
    total_recv = 0
    timeouts = 0

    while True:
        ready = select.select([sock], [], [], TIMEOUT)
        if not ready[0]:
            timeouts += 1
            if timeouts > MAX_RETRIES:
                print("\nTimeout")
                break
            if expected == 1:
                sock.sendto(protocol.build_rrq(remote_name), server)
            else:
                sock.sendto(protocol.build_ack((expected - 1) & 0xFFFF), server)
            continue

        try:
            raw, addr = sock.recvfrom(4096)
            if addr != server: continue
            
            if len(raw) < 4: continue
            opcode = struct.unpack("!H", raw[:2])[0]
            
            if opcode == protocol.DATA:
                block = struct.unpack("!H", raw[2:4])[0]
                if block == (expected & 0xFFFF):
                    data = raw[4:]
                    f.write(data)
                    total_recv += len(data)
                    
                    sock.sendto(protocol.build_ack(expected), server)
                    expected += 1
                    timeouts = 0
                    _print_progress(total_recv)
                    
                    if len(data) < protocol.MAX_DATA_LEN: break
                elif block == ((expected - 1) & 0xFFFF):
                     sock.sendto(protocol.build_ack(block), server)
            elif opcode == protocol.ERROR:
                print("Error from server")
                break
        except Exception: pass

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
    configure_socket(sock)
    
    try:
        sock.connect((host, port))
        encoded_name = remote_name.encode('utf-8')
        header = struct.pack("!BH", protocol.WRQ, len(encoded_name)) + encoded_name
        sock.sendall(header)
        
        if sock.recv(1) != b'\x01':
            print("Server rejected")
            return

        start_time = time.time()
        total_sent = 0
        while True:
            chunk = f.read(65536)
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
    configure_socket(sock)
    
    try:
        sock.connect((host, port))
        encoded_name = remote_name.encode('utf-8')
        header = struct.pack("!BH", protocol.RRQ, len(encoded_name)) + encoded_name
        sock.sendall(header)
        
        if sock.recv(1) != b'\x01':
            print("Server rejected")
            return
            
        f = open(local_path, "wb")
        start_time = time.time()
        total_recv = 0
        while True:
            chunk = sock.recv(65536)
            if not chunk: break
            f.write(chunk)
            total_recv += len(chunk)
            _print_progress(total_recv)
            
        print(f"\n[TCP] Done! Speed: {calc_speed(total_recv, time.time() - start_time)}")
    finally:
        sock.close()
        if 'f' in locals(): f.close()

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