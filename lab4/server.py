import socket
import sys
import os
import time
import struct
import multiprocessing
from datetime import datetime

# Конфигурация
HOST = '0.0.0.0'
PORT = 9999
BUFFER_SIZE = 4096

# Настройки пула процессов (Вариант 12)
N_MIN = 2   # Минимальное кол-во процессов
N_MAX = 5   # Максимальное кол-во процессов
CHECK_INTERVAL = 2.0  # Как часто менеджер проверяет нагрузку (сек)
IDLE_TIMEOUT = 10.0   # Время, после которого лишние процессы убиваются

def handle_client_connection(client_sock, client_addr):
    """Логика обработки одного клиента (из ЛР 1-3)."""
    print(f"[{os.getpid()}] Принято соединение от {client_addr}")
    
    # Включаем Keep-Alive (требование ЛР 1)
    client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    try:
        while True:
            # Получаем заголовок команды или данные
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
                # Пример: DOWNLOAD filename.txt
                if len(parts) < 2:
                    client_sock.sendall(b"Error: missing filename\n")
                    continue
                filename = parts[1]
                if not os.path.exists(filename):
                    client_sock.sendall(b"Error: file not found\n")
                    continue
                
                file_size = os.path.getsize(filename)
                client_sock.sendall(f"OK {file_size}\n".encode('utf-8'))
                
                # Замер скорости
                start_time = time.time()
                bytes_sent = 0
                
                with open(filename, 'rb') as f:
                    # sendfile эффективнее, но для лабы можно циклом
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
                # Простейшая реализация приема файла
                # Клиент шлет: UPLOAD filename size
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
                
    except ConnectionResetError:
        print(f"[{os.getpid()}] Соединение разорвано клиентом {client_addr}")
    except Exception as e:
        print(f"[{os.getpid()}] Ошибка: {e}")
    finally:
        client_sock.close()
        print(f"[{os.getpid()}] Клиент отключен")

def worker_process(server_socket, active_counter):
    """
    Функция рабочего процесса.
    Бесконечно пытается принять соединение (Parallel Accept).
    """
    print(f"Process {os.getpid()} started and waiting for connections...")
    try:
        while True:
            # Блокируемся здесь. ОС разбудит только один процесс из пула.
            try:
                client_sock, addr = server_socket.accept()
            except OSError:
                # Сокет закрыт родителем или ошибка
                break
            
            # Увеличиваем счетчик занятых процессов
            with active_counter.get_lock():
                active_counter.value += 1
                
            handle_client_connection(client_sock, addr)
            
            # Клиент отключился, уменьшаем счетчик
            with active_counter.get_lock():
                active_counter.value -= 1
                
    except KeyboardInterrupt:
        pass
    print(f"Process {os.getpid()} stopping.")

def main():
    # Создаем слушающий сокет в главном процессе
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server_socket.bind((HOST, PORT))
        server_socket.listen(N_MAX + 5)
        print(f"Server listening on {HOST}:{PORT}")
    except Exception as e:
        print(f"Bind error: {e}")
        return

    # Общая память для подсчета активных клиентов
    active_counter = multiprocessing.Value('i', 0)
    
    processes = []

    # --- Инициализация пула (N_MIN) ---
    for _ in range(N_MIN):
        p = multiprocessing.Process(target=worker_process, args=(server_socket, active_counter))
        p.daemon = True # Чтобы процессы убивались при закрытии основного
        p.start()
        processes.append(p)

    print(f"Pool initialized with {len(processes)} processes.")

    # --- Цикл Менеджера (Dynamic Pool Management) ---
    try:
        last_scale_down_time = time.time()
        
        while True:
            time.sleep(CHECK_INTERVAL)
            
            # Читаем кол-во активных соединений
            active_count = active_counter.value
            pool_size = len(processes)
            
            # Очистка мертвых процессов (на всякий случай)
            processes = [p for p in processes if p.is_alive()]
            
            print(f"[Manager] Active: {active_count}, Pool Size: {len(processes)}")

            # 1. Логика расширения (Scale Up)
            # Если все процессы заняты и мы не достигли лимита
            if active_count >= pool_size and pool_size < N_MAX:
                print(f"[Manager] Load high! Spawning new worker.")
                p = multiprocessing.Process(target=worker_process, args=(server_socket, active_counter))
                p.daemon = True
                p.start()
                processes.append(p)
                last_scale_down_time = time.time() # Сбрасываем таймер уменьшения

            # 2. Логика сжатия (Scale Down)
            # Если активных сильно меньше, чем процессов, и прошло время
            elif active_count < (pool_size - 1) and pool_size > N_MIN:
                if time.time() - last_scale_down_time > IDLE_TIMEOUT:
                    print(f"[Manager] Load low. Terminating extra worker.")
                    # Убиваем последний процесс (грубый метод, но для лабы подходит)
                    # В продакшене лучше посылать сигнал "завершись после обработки"
                    victim = processes.pop()
                    victim.terminate()
                    victim.join()
                    last_scale_down_time = time.time()

    except KeyboardInterrupt:
        print("\nServer shutting down...")
    finally:
        server_socket.close()
        for p in processes:
            p.terminate()
            p.join()

if __name__ == '__main__':
    main()