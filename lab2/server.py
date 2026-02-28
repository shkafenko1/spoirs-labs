import argparse
import os
import socket
import time
import select
import struct
from typing import Tuple, Dict

import protocol

WINDOW_SIZE = 64
TIMEOUT = 0.1
MAX_RETRIES = 20
SOCKET_BUF_SIZE = 1 * 1024 * 1024  # 1 MB

def configure_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUF_SIZE)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUF_SIZE)
    except:
        pass

# --- UDP LOGIC ---

def udp_send_file(sock: socket.socket, client_addr: Tuple[str, int], filepath: str):
    try:
        f = open(filepath, "rb")
    except OSError:
        sock.sendto(protocol.build_error(protocol.ERR_FILE_NOT_FOUND, "File not found"), client_addr)
        return

    configure_socket(sock)
    start_time = time.time()
    
    base = 1
    next_seq = 1
    window_buff: Dict[int, bytes] = {}
    eof = False
    total_sent = 0
    consecutive_timeouts = 0

    with f:
        while base <= next_seq or not eof:
            # 1. Fill Window
            while next_seq < base + WINDOW_SIZE and not eof:
                chunk = f.read(protocol.MAX_DATA_LEN)
                if not chunk:
                    eof = True
                    break
                if len(chunk) < protocol.MAX_DATA_LEN:
                    eof = True
                
                pkt = protocol.build_data(next_seq, chunk)
                window_buff[next_seq] = pkt
                sock.sendto(pkt, client_addr)
                total_sent += len(chunk)
                next_seq += 1

            if eof and base == next_seq:
                break

            # 2. Wait for ACK
            ready = select.select([sock], [], [], TIMEOUT)
            if ready[0]:
                try:
                    raw, addr = sock.recvfrom(4096)
                    if addr != client_addr: continue
                    
                    if len(raw) >= 4:
                        opcode = struct.unpack("!H", raw[:2])[0]
                        if opcode == protocol.ACK:
                            ack_val = struct.unpack("!H", raw[2:4])[0]
                            # Handle wrap-around or simple linear logic
                            # For lab simple logic: assuming no 64k wrap-around collisions in small window
                            if ack_val >= (base & 0xFFFF):
                                shift = ack_val - (base & 0xFFFF) + 1
                                for _ in range(shift):
                                    if base in window_buff: del window_buff[base]
                                    base += 1
                                consecutive_timeouts = 0
                        elif opcode == protocol.ERROR:
                            return
                except Exception: pass
            else:
                consecutive_timeouts += 1
                if consecutive_timeouts > MAX_RETRIES:
                    return
                # Retransmit window
                for seq in range(base, next_seq):
                    if seq in window_buff:
                        sock.sendto(window_buff[seq], client_addr)

    duration = time.time() - start_time
    speed = (total_sent * 8 / 1_000_000) / (duration if duration > 0 else 1)
    print(f"[UDP] Upload finished: {total_sent} bytes to {client_addr} at {speed:.2f} Mbps")

def udp_recv_file(sock: socket.socket, client_addr: Tuple[str, int], filepath: str, allow_overwrite: bool):
    if not allow_overwrite and os.path.exists(filepath):
        sock.sendto(protocol.build_error(protocol.ERR_FILE_EXISTS, "File already exists"), client_addr)
        return
    try:
        f = open(filepath, "wb")
    except OSError:
        sock.sendto(protocol.build_error(protocol.ERR_ACCESS_VIOLATION, "Cannot create file"), client_addr)
        return

    configure_socket(sock)
    sock.sendto(protocol.build_ack(0), client_addr)

    start_time = time.time()
    expected_block = 1
    total_received = 0
    timeouts = 0

    while True:
        ready = select.select([sock], [], [], TIMEOUT)
        if not ready[0]:
            timeouts += 1
            if timeouts > MAX_RETRIES: break
            # Resend last ACK
            sock.sendto(protocol.build_ack((expected_block - 1) & 0xFFFF), client_addr)
            continue

        try:
            raw, addr = sock.recvfrom(4096)
            if addr != client_addr: continue
            
            if len(raw) < 4: continue
            opcode = struct.unpack("!H", raw[:2])[0]
            
            if opcode == protocol.DATA:
                block = struct.unpack("!H", raw[2:4])[0]
                if block == (expected_block & 0xFFFF):
                    data = raw[4:]
                    f.write(data)
                    total_received += len(data)
                    
                    sock.sendto(protocol.build_ack(expected_block), client_addr)
                    expected_block += 1
                    timeouts = 0
                    
                    if len(data) < protocol.MAX_DATA_LEN:
                        break
                elif block == ((expected_block - 1) & 0xFFFF):
                    sock.sendto(protocol.build_ack(block), client_addr)
        except Exception: pass

    f.close()
    duration = time.time() - start_time
    speed = (total_received * 8 / 1_000_000) / (duration if duration > 0 else 1)
    print(f"[UDP] Download finished: {total_received} bytes from {client_addr} at {speed:.2f} Mbps")

def run_udp_server(host: str, port: int, root: str, allow_overwrite: bool):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    configure_socket(sock)
    print(f"UDP Server listening on {host}:{port}")

    while True:
        try:
            data, addr = sock.recvfrom(4096)
            if len(data) < 2: continue
            opcode = struct.unpack("!H", data[:2])[0]
            
            # Simple parse just to get filename
            parsed = protocol.parse(data)
        except Exception: continue

        if opcode == protocol.RRQ:
            safe_path = os.path.join(root, os.path.basename(parsed.filename))
            udp_send_file(sock, addr, safe_path)
        elif opcode == protocol.WRQ:
            safe_path = os.path.join(root, os.path.basename(parsed.filename))
            udp_recv_file(sock, addr, safe_path, allow_overwrite)
        else:
            sock.sendto(protocol.build_error(protocol.ERR_ILLEGAL_OP, "Expected RRQ/WRQ"), addr)

# --- TCP LOGIC ---
def run_tcp_server(host: str, port: int, root: str, allow_overwrite: bool):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUF_SIZE)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUF_SIZE)
    sock.bind((host, port))
    sock.listen(5)
    print(f"TCP Server listening on {host}:{port}")
    
    while True:
        conn, addr = sock.accept()
        try:
            header = conn.recv(3)
            if not header: continue
            opcode, name_len = struct.unpack("!BH", header)
            filename = conn.recv(name_len).decode("utf-8")
            safe_path = os.path.join(root, os.path.basename(filename))

            if opcode == protocol.WRQ:
                if not allow_overwrite and os.path.exists(safe_path):
                    conn.sendall(b'\x00')
                    conn.close()
                    continue
                conn.sendall(b'\x01')
                with open(safe_path, "wb") as f:
                    while True:
                        data = conn.recv(65536)
                        if not data: break
                        f.write(data)
            elif opcode == protocol.RRQ:
                if not os.path.exists(safe_path):
                    conn.sendall(b'\x00')
                    conn.close()
                    continue
                conn.sendall(b'\x01')
                with open(safe_path, "rb") as f:
                    while True:
                        chunk = f.read(65536)
                        if not chunk: break
                        conn.sendall(chunk)
        except Exception: pass
        finally: conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6969)
    parser.add_argument("--dir", default="server_files")
    parser.add_argument("--allow-overwrite", action="store_true")
    parser.add_argument("--protocol", choices=["tcp", "udp"], default="udp")
    args = parser.parse_args()
    
    if not os.path.exists(args.dir): os.makedirs(args.dir)

    if args.protocol == "udp":
        run_udp_server(args.host, args.port, args.dir, args.allow_overwrite)
    else:
        run_tcp_server(args.host, args.port, args.dir, args.allow_overwrite)