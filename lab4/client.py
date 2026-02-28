import socket
import sys
import os
import time

# Порт должен совпадать с сервером
PORT = 9999

def send_file(sock, filename):
    if not os.path.exists(filename):
        print("Файл не найден")
        return
    filesize = os.path.getsize(filename)
    sock.sendall(f"UPLOAD {filename} {filesize}\n".encode())
    
    resp = sock.recv(1024).decode()
    if "READY" not in resp:
        print(f"Сервер отклонил: {resp}")
        return

    print("Отправка данных...")
    with open(filename, 'rb') as f:
        while True:
            data = f.read(4096)
            if not data: break
            sock.sendall(data)
    
    print(sock.recv(1024).decode())

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Запрос IP адреса у пользователя
    print("Ведите IP сервера (нажмите Enter для 127.0.0.1):")
    target_ip = input("> ").strip()
    if not target_ip:
        target_ip = '127.0.0.1'

    try:
        print(f"Попытка подключения к {target_ip}:{PORT}...")
        s.connect((target_ip, PORT))
        print(f"Успешно подключено!")
        print("Команды: ECHO <текст>, TIME, UPLOAD <файл>, DOWNLOAD <файл>, CLOSE")
        
        while True:
            cmd = input("Command> ")
            if not cmd: continue
            
            if cmd.startswith("UPLOAD"):
                parts = cmd.split()
                if len(parts) > 1:
                    send_file(s, parts[1])
                else:
                    print("Usage: UPLOAD filename")
                continue

            s.sendall((cmd + "\n").encode())
            
            if cmd.strip().upper() == 'CLOSE':
                break
                
            data = s.recv(4096)
            print("Server:", data.decode(errors='ignore').strip())
            
    except ConnectionRefusedError:
        print("Ошибка: Не удалось подключиться. Проверьте IP и запущен ли сервер.")
    except TimeoutError:
        print("Ошибка: Таймаут. Возможно, Брандмауэр блокирует порт 9999 на сервере.")
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        s.close()

if __name__ == '__main__':
    main()