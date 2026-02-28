import argparse
import os
import socket
import time
import select
from typing import Tuple, Dict

import protocol

WINDOW_SIZE = 16
TIMEOUT = 0.5
MAX_RETRIES = 10

def send_file_windowed(sock: socket.socket, client_addr: Tuple[str, int], filepath: str):
    try:
        f = open(filepath, "rb")
    except OSError:
        sock.sendto(protocol.build_error(protocol.ERR_FILE_NOT_FOUND, "File not found"), client_addr)
        return

    start_time = time.time()
    
    base = 1
    next_seq = 1
    window_buff: Dict[int, bytes] = {}
    eof = False
    
    total_sent = 0
    consecutive_timeouts = 0

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
                window_buff[next_seq] = pkt
                sock.sendto(pkt, client_addr)
                total_sent += len(chunk)
                next_seq += 1

            if eof and base == next_seq:
                break

            ready = select.select([sock], [], [], TIMEOUT)
            if ready[0]:
                try:
                    raw, addr = sock.recvfrom(4096)
                    if addr != client_addr: continue
                    
                    parsed = protocol.parse(raw)
                    if parsed.opcode == protocol.ERROR:
                        return
                    
                    if parsed.opcode == protocol.ACK:
                        ack_val = parsed.block
                        if ack_val >= (base & 0xFFFF):
                             shift = ack_val - (base & 0xFFFF) + 1
                             for _ in range(shift):
                                 if base in window_buff: del window_buff[base]
                                 base += 1
                             consecutive_timeouts = 0
                except protocol.ProtocolError:
                    pass
            else:
                consecutive_timeouts += 1
                if consecutive_timeouts > MAX_RETRIES:
                    return
                
                for seq in range(base, next_seq):
                    if seq in window_buff:
                        sock.sendto(window_buff[seq], client_addr)

    duration = time.time() - start_time
    speed = (total_sent * 8 / 1_000_000) / (duration if duration > 0 else 1)
    print(f"Transfer finished: {total_sent} bytes to {client_addr} at {speed:.2f} Mbps")


def recv_file_windowed(sock: socket.socket, client_addr: Tuple[str, int], filepath: str, allow_overwrite: bool):
    if not allow_overwrite and os.path.exists(filepath):
        sock.sendto(protocol.build_error(protocol.ERR_FILE_EXISTS, "File already exists"), client_addr)
        return

    try:
        f = open(filepath, "wb")
    except OSError:
        sock.sendto(protocol.build_error(protocol.ERR_ACCESS_VIOLATION, "Cannot create file"), client_addr)
        return

    start_time = time.time()
    sock.sendto(protocol.build_ack(0), client_addr)

    expected_block = 1
    total_received = 0
    retries = 0

    while True:
        ready = select.select([sock], [], [], TIMEOUT)
        if not ready[0]:
            retries += 1
            if retries > MAX_RETRIES:
                break
            sock.sendto(protocol.build_ack((expected_block - 1) & 0xFFFF), client_addr)
            continue

        try:
            raw, addr = sock.recvfrom(4096)
            if addr != client_addr: continue
            
            pkt = protocol.parse(raw)
        except protocol.ProtocolError:
            continue

        if pkt.opcode == protocol.DATA:
            if pkt.block == (expected_block & 0xFFFF):
                f.write(pkt.data)
                total_received += len(pkt.data)
                
                sock.sendto(protocol.build_ack(expected_block), client_addr)
                expected_block += 1
                retries = 0

                if len(pkt.data) < protocol.MAX_DATA_LEN:
                    break
            elif pkt.block == ((expected_block - 1) & 0xFFFF):
                sock.sendto(protocol.build_ack(pkt.block), client_addr)
            
    f.close()
    duration = time.time() - start_time
    speed = (total_received * 8 / 1_000_000) / (duration if duration > 0 else 1)
    print(f"Receive finished: {total_received} bytes from {client_addr} at {speed:.2f} Mbps")

def run_server(host: str, port: int, root: str, allow_overwrite: bool):
    if not os.path.exists(root):
        os.makedirs(root)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    print(f"UDP Server listening on {host}:{port}, root: {root}")

    while True:
        try:
            data, addr = sock.recvfrom(4096)
            pkt = protocol.parse(data)
        except (socket.error, protocol.ProtocolError):
            continue

        if pkt.opcode == protocol.RRQ:
            safe_path = os.path.join(root, os.path.basename(pkt.filename))
            send_file_windowed(sock, addr, safe_path)
        
        elif pkt.opcode == protocol.WRQ:
            safe_path = os.path.join(root, os.path.basename(pkt.filename))
            recv_file_windowed(sock, addr, safe_path, allow_overwrite)
        
        else:
            sock.sendto(protocol.build_error(protocol.ERR_ILLEGAL_OP, "Expected RRQ/WRQ"), addr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6969)
    parser.add_argument("--root", default="server_files")
    parser.add_argument("--allow-overwrite", action="store_true")
    args = parser.parse_args()
    
    run_server(args.host, args.port, args.root, args.allow_overwrite)