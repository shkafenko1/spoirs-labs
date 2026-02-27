import argparse
import logging
import os
import socket
import time
import select
from typing import Tuple, Dict

import protocol

WINDOW_SIZE = 16
TIMEOUT = 0.5
MAX_RETRIES = 10

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S"
    )

def send_file_windowed(sock: socket.socket, client_addr: Tuple[str, int], filepath: str):
    try:
        f = open(filepath, "rb")
        file_size = os.path.getsize(filepath)
    except OSError:
        sock.sendto(protocol.build_error(protocol.ERR_FILE_NOT_FOUND, "File not found"), client_addr)
        return

    logging.info(f"Start sending {filepath} ({file_size} bytes) to {client_addr}")
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
                        logging.error(f"Client reported error: {parsed.error_msg}")
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
                    logging.error("Connection lost (Timeout)")
                    return
                
                for seq in range(base, next_seq):
                    if seq in window_buff:
                        sock.sendto(window_buff[seq], client_addr)

    duration = time.time() - start_time
    speed = (total_sent * 8 / 1_000_000) / (duration if duration > 0 else 1)
    logging.info(f"Done. Sent {total_sent} bytes. Speed: {speed:.2f} Mbps")


def recv_file_windowed(sock: socket.socket, client_addr: Tuple[str, int], filepath: str):
    try:
        f = open(filepath, "wb")
    except OSError:
        sock.sendto(protocol.build_error(protocol.ERR_ACCESS_VIOLATION, "Cannot create file"), client_addr)
        return

    logging.info(f"Start receiving to {filepath} from {client_addr}")
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
                logging.error("Connection lost (Timeout)")
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
                    logging.info("End of file received.")
                    break
            elif pkt.block == ((expected_block - 1) & 0xFFFF):
                sock.sendto(protocol.build_ack(pkt.block), client_addr)
            
    f.close()
    duration = time.time() - start_time
    speed = (total_received * 8 / 1_000_000) / (duration if duration > 0 else 1)
    logging.info(f"Done. Received {total_received} bytes. Speed: {speed:.2f} Mbps")

def run_server(host: str, port: int, storage_dir: str):
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    logging.info(f"UDP Server started on {host}:{port}, root: {storage_dir}")

    while True:
        try:
            data, addr = sock.recvfrom(4096)
            pkt = protocol.parse(data)
        except (socket.error, protocol.ProtocolError) as e:
            continue

        if pkt.opcode == protocol.RRQ:
            safe_path = os.path.join(storage_dir, os.path.basename(pkt.filename))
            send_file_windowed(sock, addr, safe_path)
        
        elif pkt.opcode == protocol.WRQ:
            safe_path = os.path.join(storage_dir, os.path.basename(pkt.filename))
            recv_file_windowed(sock, addr, safe_path)
        
        else:
            sock.sendto(protocol.build_error(protocol.ERR_ILLEGAL_OP, "Expected RRQ/WRQ"), addr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6000)
    parser.add_argument("--dir", default="server_files")
    args = parser.parse_args()
    
    setup_logging()
    run_server(args.host, args.port, args.dir)