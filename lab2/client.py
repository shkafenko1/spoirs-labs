import argparse
import socket
import os
import time
import select
import sys
import struct
from typing import Dict

import protocol

# --- CONFIG FOR SPEED ---
WINDOW_SIZE = 512
TIMEOUT = 0.05
MAX_RETRIES = 20
SOCKET_BUF_SIZE = 4 * 1024 * 1024 

def configure_fast_socket(sock: socket.socket):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUF_SIZE)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUF_SIZE)

def _print_progress(bytes_transferred):
    # Выводим реже, чтобы не тормозить консоль
    if bytes_transferred % (1024 * 1024) < 2000: 
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
    configure_fast_socket(sock)
    
    try:
        f = open(local_path, "rb")
        file_size = os.path.getsize(local_path)
    except OSError:
        print("Error reading file")
        return

    print(f"[UDP] Uploading {local_path} ({file_size} bytes)")
    sock.sendto(protocol.build_wrq(remote_name), server)
    
    # Handshake wait
    sock.settimeout(1.0)
    try:
        raw, _ = sock.recvfrom(1024)
        if protocol.parse(raw).opcode != protocol.ACK: return
    except socket.timeout:
        print("Server unreachable")
        return

    sock.setblocking(False)
    start_time = time.time()
    
    base = 1
    next_seq = 1
    window_buff: Dict[int, bytes] = {}
    eof = False
    total_sent = 0
    last_ack_time = time.time()

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
                try:
                    sock.sendto(pkt, server)
                except BlockingIOError: break
                
                total_sent += len(chunk)
                next_seq += 1
            
            _print_progress(total_sent)

            # Fast ACK check
            try:
                while True:
                    raw, _ = sock.recvfrom(1024)
                    # Quick parse
                    if len(raw) < 4: continue
                    # opcode check
                    if raw[0] == 0 and raw[1] == protocol.ACK:
                        ack_val = raw[2]*256 + raw[3]
                        if ack_val >= (base & 0xFFFF):
                            shift = ack_val - (base & 0xFFFF) + 1
                            if shift < 0: shift += 65536
                            for _ in range(shift):
                                if base in window_buff: del window_buff[base]
                                base += 1
                            last_ack_time = time.time()
            except BlockingIOError: pass

            if time.time() - last_ack_time > TIMEOUT:
                if base in window_buff:
                    try:
                        sock.sendto(window_buff[base], server)
                    except BlockingIOError: pass
                last_ack_time = time.time()

            if next_seq >= base + WINDOW_SIZE:
                time.sleep(0.001)

    print(f"\n[UDP] Done! Speed: {calc_speed(total_sent, time.time() - start_time)}")
    sock.close()

def udp_download(host: str, port: int, remote_name: str, local_path: str):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server = (host, port)
    configure_fast_socket(sock)
    
    print(f"[UDP] Downloading {remote_name}")
    sock.sendto(protocol.build_rrq(remote_name), server)
    
    try:
        f = open(local_path, "wb")
    except OSError: return

    sock.setblocking(False)
    start_time = time.time()
    expected = 1
    total_recv = 0
    last_act_time = time.time()

    while True:
        try:
            raw, addr = sock.recvfrom(4096)
            if addr != server: continue
            
            if len(raw) < 4: continue
            opcode = raw[0]*256 + raw[1]
            
            if opcode == protocol.DATA:
                block = raw[2]*256 + raw[3]
                if block == (expected & 0xFFFF):
                    data = raw[4:]
                    f.write(data)
                    total_recv += len(data)
                    
                    ack = struct.pack("!HH", protocol.ACK, block)
                    sock.sendto(ack, server)
                    
                    expected += 1
                    last_act_time = time.time()
                    _print_progress(total_recv)
                    
                    if len(data) < protocol.MAX_DATA_LEN: break
                elif block == ((expected - 1) & 0xFFFF):
                     ack = struct.pack("!HH", protocol.ACK, block)
                     sock.sendto(ack, server)
                     last_act_time = time.time()
            elif opcode == protocol.ERROR:
                print("Error from server")
                break
        except BlockingIOError:
            if time.time() - last_act_time > 1.0:
                # Resend request if silent
                if expected == 1: sock.sendto(protocol.build_rrq(remote_name), server)
                else: sock.sendto(struct.pack("!HH", protocol.ACK, (expected-1)&0xFFFF), server)
                last_act_time = time.time()
            time.sleep(0.0001)

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
    configure_fast_socket(sock)
    
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
    configure_fast_socket(sock)
    
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