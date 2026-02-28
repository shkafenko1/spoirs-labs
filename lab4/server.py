import socket
import sys
import os
import time
import struct
import multiprocessing
from datetime import datetime

# Конфигурация
HOST = '0.0.0.0'  # Слушаем все сетевые интерфейсы
PORT = 9999
BUFFER_SIZE = 4096

# Настройки пула процессов (Вариант 12)
N_MIN = 2
N_MAX = 5
CHECK_INTERVAL = 2.0
IDLE_TIMEOUT = 10.0

def get_local_ip():
    """Определяет локальный IP компьютера в сети."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Неважно, есть ли интернет, этот адрес не достигается, 
        # но ОС выберет правильный интерфейс для маршрутизации
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

def handle_client_connection(client_sock, client_addr):
    """Логика обработки одного клиента."""
    print(f"[{os.getpid()}] Принято соединение от {client_addr}")
    client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    try:
        while True:
            data = client_sock.recv(BUFFER_SIZE)
            if not data:
                break
            
            command_line = data.decode('utf-8', errors='ignore').strip()
            parts = command_line.split()
            if not parts:
                continue
            
            cmd = parts[0].upper()
            
            if cmd == 'ECHO':
                response = " ".join(parts[1:]) + "\n"
                client_sock.sendall(response.encode('utf-8'))
                
            elif cmd == 'TIME':
                response = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
                client_sock.sendall(response.encode('utf-8'))
                
            elif cmd == 'CLOSE' or cmd == 'EXIT':
                client_sock.sendall(b"Goodbye\n")
                break

            elif cmd == 'DOWNLOAD':
                if len(parts) < 2:
                    client_sock.sendall(b"Error: missing filename\n")
                    continue
                filename = parts[1]
                if not os.path.exists(filename):
                    client_sock.sendall(b"Error: file not found\n")
                    continue
                
                file_size = os.path.getsize(filename)
                client_sock.sendall(f"OK {file_size}\n".encode('utf-8'))
                
                start_time = time.time()
                bytes_sent = 0
                with open(filename, 'rb') as f:
                    while True:
                        chunk = f.read(BUFFER_SIZE)
                        if not chunk:
                            break
                        client_sock.sendall(chunk)
                        bytes_sent += len(chunk)
                        
                duration = time.time() - start_time
                bitrate = (bytes_sent * 8) / (duration if duration > 0 else 0.001)
                print(f"[{os.getpid()}] Sent {filename}: {bitrate/1000000:.2f} Mbps")
                
            elif cmd == 'UPLOAD':
                if len(parts) < 3:
                    client_sock.sendall(b"Error: usage UPLOAD name size\n")
                    continue
                
                fname = os.path.basename(parts[1]) + "_uploaded"
                try:
                    fsize = int(parts[2])
                except ValueError:
                    client_sock.sendall(b"Error: invalid size\n")
                    continue

                client_sock.sendall(b"READY\n")
                
                received = 0
                start_time = time.time()
                with open(fname, 'wb') as f:
                    while received < fsize:
                        chunk = client_sock.recv(min(BUFFER_SIZE, fsize - received))
                        if not chunk:
                            break
                        f.write(chunk)
                        received += len(chunk)
                
                duration = time.time() - start_time
                bitrate = (received * 8) / (duration if duration > 0 else 0.001)
                msg = f"Uploaded. Bitrate: {bitrate/1000000:.2f} Mbps\n"
                client_sock.sendall(msg.encode('utf-8'))
            else:
                client_sock.sendall(b"Unknown command\n")
                
    except Exception as e:
        print(f"[{os.getpid()}] Error: {e}")
    finally:
        client_sock.close()
        print(f"[{os.getpid()}] Client disconnected")

def worker_process(server_socket, active_counter):
    """Рабочий процесс: Parallel Accept"""
    try:
        while True:
            try:
                client_sock, addr = server_socket.accept()
            except OSError:
                break
            
            with active_counter.get_lock():
                active_counter.value += 1
                
            handle_client_connection(client_sock, addr)
            
            with active_counter.get_lock():
                active_counter.value -= 1
    except KeyboardInterrupt:
        pass

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind((HOST, PORT))
        server_socket.listen(N_MAX + 5)
    except Exception as e:
        print(f"Ошибка запуска: {e}")
        return

    my_ip = get_local_ip()
    print("="*50)
    print(f"СЕРВЕР ЗАПУЩЕН.")
    print(f"Ваш IP в локальной сети: {my_ip}")
    print(f"На клиенте введите этот IP для подключения.")
    print("="*50)

    active_counter = multiprocessing.Value('i', 0)
    processes = []

    for _ in range(N_MIN):
        p = multiprocessing.Process(target=worker_process, args=(server_socket, active_counter))
        p.daemon = True
        p.start()
        processes.append(p)

    try:
        last_scale_down_time = time.time()
        while True:
            time.sleep(CHECK_INTERVAL)
            active_count = active_counter.value
            pool_size = len(processes)
            processes = [p for p in processes if p.is_alive()]
            
            print(f"[Manager] Active: {active_count}, Pool: {pool_size}")

            if active_count >= pool_size and pool_size < N_MAX:
                print(f"[Manager] Spawning worker.")
                p = multiprocessing.Process(target=worker_process, args=(server_socket, active_counter))
                p.daemon = True
                p.start()
                processes.append(p)
                last_scale_down_time = time.time()
            elif active_count < (pool_size - 1) and pool_size > N_MIN:
                if time.time() - last_scale_down_time > IDLE_TIMEOUT:
                    print(f"[Manager] Terminating worker.")
                    victim = processes.pop()
                    victim.terminate()
                    victim.join()
                    last_scale_down_time = time.time()

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        server_socket.close()
        for p in processes:
            p.terminate()

if __name__ == '__main__':
    main()