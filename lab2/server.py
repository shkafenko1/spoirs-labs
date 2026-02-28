import argparse
import os
import socket
import time
import select
import struct
from typing import Tuple, Dict

import protocol

# --- CONFIG FOR SPEED ---
WINDOW_SIZE = 512       # Огромное окно для высокой скорости
TIMEOUT = 0.05          # Очень короткий таймаут для быстрой реакции
MAX_RETRIES = 20
SOCKET_BUF_SIZE = 4 * 1024 * 1024  # 4 MB буфер сокета (ОЧЕНЬ ВАЖНО)

def configure_fast_socket(sock: socket.socket):
    """Расширяем системные буферы, чтобы ядро не отбрасывало пакеты"""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUF_SIZE)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUF_SIZE)

# --- UDP LOGIC ---

def udp_send_file(sock: socket.socket, client_addr: Tuple[str, int], filepath: str):
    try:
        f = open(filepath, "rb")
    except OSError:
        sock.sendto(protocol.build_error(protocol.ERR_FILE_NOT_FOUND, "File not found"), client_addr)
        return

    configure_fast_socket(sock)
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
            # 1. Burst Sending (Заполняем окно до отказа)
            while next_seq < base + WINDOW_SIZE and not eof:
                chunk = f.read(protocol.MAX_DATA_LEN)
                if not chunk:
                    eof = True
                    break
                if len(chunk) < protocol.MAX_DATA_LEN:
                    eof = True
                
                pkt = protocol.build_data(next_seq, chunk)
                window_buff[next_seq] = pkt
                try:
                    sock.sendto(pkt, client_addr)
                except BlockingIOError:
                    break # Сокет переполнен, ждем
                
                total_sent += len(chunk)
                next_seq += 1

            # 2. Non-blocking ACK receive
            try:
                while True: # Читаем все накопившиеся ACK
                    raw, addr = sock.recvfrom(4096)
                    if addr != client_addr: continue
                    parsed = protocol.parse(raw)
                    
                    if parsed.opcode == protocol.ACK:
                        ack_val = parsed.block
                        # Cumulative ACK logic
                        if ack_val >= (base & 0xFFFF):
                            shift = ack_val - (base & 0xFFFF) + 1
                            # Корректировка на переполнение (упрощенно)
                            if shift < 0: shift += 65536 
                            
                            # Сдвигаем окно
                            for _ in range(shift):
                                if base in window_buff: del window_buff[base]
                                base += 1
                            last_ack_time = time.time()
                    elif parsed.opcode == protocol.ERROR:
                        return
            except BlockingIOError:
                pass # Нет данных, не страшно

            # 3. Timeout & Retransmission
            if time.time() - last_ack_time > TIMEOUT:
                # Если застряли - перепосылаем только базу, чтобы "пробить" затор
                if base in window_buff:
                    try:
                        sock.sendto(window_buff[base], client_addr)
                    except BlockingIOError: pass
                last_ack_time = time.time() # Сброс таймера, чтобы не спамить

            # Небольшой yield, чтобы не вешать CPU на 100%, если окно полно
            if next_seq >= base + WINDOW_SIZE:
                time.sleep(0.001)

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

    configure_fast_socket(sock)
    sock.setblocking(False)

    start_time = time.time()
    # Отправляем ACK 0 несколько раз для надежности старта
    for _ in range(3):
        sock.sendto(protocol.build_ack(0), client_addr)
    
    expected_block = 1
    total_received = 0
    last_act_time = time.time()

    while True:
        try:
            raw, addr = sock.recvfrom(4096)
            if addr != client_addr: continue
            
            # Парсим вручную для скорости (можно оптимизировать parse, но пока так)
            # opcode(2) + block(2)
            if len(raw) < 4: continue
            opcode = raw[0] * 256 + raw[1] # !H
            
            if opcode == protocol.DATA:
                block = raw[2] * 256 + raw[3] # !H
                
                if block == (expected_block & 0xFFFF):
                    data = raw[4:]
                    f.write(data)
                    total_received += len(data)
                    
                    # ACK
                    ack_pkt = struct.pack("!HH", protocol.ACK, expected_block & 0xFFFF)
                    sock.sendto(ack_pkt, client_addr)
                    
                    expected_block += 1
                    last_act_time = time.time()
                    
                    if len(data) < protocol.MAX_DATA_LEN:
                        break
                elif block == ((expected_block - 1) & 0xFFFF):
                    # Повтор ACK для предыдущего
                    ack_pkt = struct.pack("!HH", protocol.ACK, block)
                    sock.sendto(ack_pkt, client_addr)
                    last_act_time = time.time()
        
        except BlockingIOError:
            # Если данных нет долго - проверяем таймаут
            if time.time() - last_act_time > 2.0: # 2 сек тишины - выход
                print("Receive timeout")
                break
            time.sleep(0.0001) # Микро-сон, чтобы не жарить CPU
            
    f.close()
    duration = time.time() - start_time
    speed = (total_received * 8 / 1_000_000) / (duration if duration > 0 else 1)
    print(f"[UDP] Download finished: {total_received} bytes from {client_addr} at {speed:.2f} Mbps")

def run_udp_server(host: str, port: int, root: str, allow_overwrite: bool):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    configure_fast_socket(sock)
    print(f"Fast UDP Server listening on {host}:{port}")

    while True:
        try:
            data, addr = sock.recvfrom(4096)
            # Простой парсинг для диспетчера
            if len(data) < 2: continue
            opcode = struct.unpack("!H", data[:2])[0]
            
            parsed = protocol.parse(data)
        except Exception: continue

        if opcode == protocol.RRQ:
            safe_path = os.path.join(root, os.path.basename(parsed.filename))
            udp_send_file(sock, addr, safe_path)
        elif opcode == protocol.WRQ:
            safe_path = os.path.join(root, os.path.basename(parsed.filename))
            udp_recv_file(sock, addr, safe_path, allow_overwrite)

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