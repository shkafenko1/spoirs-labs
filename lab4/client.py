import socket
import sys
import os
import time

PORT = 9999

def send_file(sock, filename):
    """Отправка файла на сервер (UPLOAD)"""
    if not os.path.exists(filename):
        print(f"Ошибка: Файл '{filename}' не найден")
        return
    filesize = os.path.getsize(filename)
    
    # 1. Отправляем заголовок
    sock.sendall(f"UPLOAD {filename} {filesize}\n".encode())
    
    # 2. Ждем подтверждения от сервера (READY)
    # Читаем аккуратно до переноса строки
    resp = b""
    while b"\n" not in resp:
        chunk = sock.recv(1)
        if not chunk: break
        resp += chunk
        
    if b"READY" not in resp:
        print(f"Сервер отклонил загрузку: {resp.decode().strip()}")
        return

    print(f"Отправка файла {filename} ({filesize} байт)...")
    with open(filename, 'rb') as f:
        sent_total = 0
        while True:
            data = f.read(4096)
            if not data: break
            sock.sendall(data)
            sent_total += len(data)
    
    # Ждем финального ответа (битрейт)
    print(sock.recv(1024).decode().strip())

def download_file(sock, filename):
    """Скачивание файла с сервера (DOWNLOAD)"""
    # 1. Отправляем команду
    sock.sendall(f"DOWNLOAD {filename}\n".encode())

    # 2. Читаем заголовок ответа (например: "OK 54321\n" или "Error...")
    # Читаем побайтово до \n, чтобы не прочитать случайно кусок самого файла
    header_line = b""
    while True:
        chunk = sock.recv(1)
        if not chunk:
            print("Сервер разорвал соединение.")
            return
        header_line += chunk
        if chunk == b'\n':
            break
    
    header = header_line.decode().strip()
    
    # Проверяем, есть ли OK
    if not header.startswith("OK"):
        print(f"Ошибка сервера: {header}")
        return
    
    try:
        # Парсим размер файла (формат: "OK <размер>")
        filesize = int(header.split()[1])
    except (IndexError, ValueError):
        print("Ошибка протокола: сервер прислал некорректный размер.")
        return

    print(f"Скачивание {filename} ({filesize} байт)...")
    
    # 3. Читаем само тело файла
    save_name = f"saved_{filename}" # Чтобы не затереть оригинал при тесте
    received_total = 0
    
    start_time = time.time()
    
    with open(save_name, 'wb') as f:
        while received_total < filesize:
            # Читаем порциями, но не больше, чем осталось
            to_read = min(4096, filesize - received_total)
            chunk = sock.recv(to_read)
            if not chunk:
                print("Ошибка: соединение прервано во время загрузки.")
                break
            f.write(chunk)
            received_total += len(chunk)

    duration = time.time() - start_time
    # Расчет скорости
    if duration == 0: duration = 0.001
    mbps = (received_total * 8) / (duration * 1_000_000)
    
    print(f"Файл успешно сохранен как '{save_name}'")
    print(f"Скорость скачивания: {mbps:.2f} Mbps")

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    print("Введите IP сервера (Enter для 127.0.0.1):")
    target_ip = input("> ").strip() or '127.0.0.1'

    try:
        print(f"Подключение к {target_ip}:{PORT}...")
        s.connect((target_ip, PORT))
        print("Подключено!")
        print("Доступные команды:")
        print("  ECHO <текст>      - эхо-тест")
        print("  TIME              - время на сервере")
        print("  UPLOAD <файл>     - отправить файл на сервер")
        print("  DOWNLOAD <файл>   - скачать файл с сервера")
        print("  CLOSE             - выход")
        print("-" * 40)
        
        while True:
            cmd_input = input("Command> ").strip()
            if not cmd_input: continue
            
            # Разбираем команду
            parts = cmd_input.split()
            command = parts[0].upper()
            
            if command == 'UPLOAD':
                if len(parts) > 1:
                    send_file(s, parts[1])
                else:
                    print("Использование: UPLOAD filename.txt")
                continue

            elif command == 'DOWNLOAD':
                if len(parts) > 1:
                    download_file(s, parts[1])
                else:
                    print("Использование: DOWNLOAD filename.txt")
                continue

            elif command == 'CLOSE' or command == 'EXIT':
                s.sendall(b"CLOSE\n")
                break

            # Для остальных команд (ECHO, TIME)
            s.sendall((cmd_input + "\n").encode())
            
            # Читаем ответ (простой текст)
            response = s.recv(4096)
            print("Server:", response.decode(errors='ignore').strip())
            
    except ConnectionRefusedError:
        print("Не удалось подключиться. Сервер не запущен?")
    except KeyboardInterrupt:
        print("\nВыход.")
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        s.close()

if __name__ == '__main__':
    main()