import socket
import sys
import os
import time

HOST = '127.0.0.1'
PORT = 9999

def send_file(sock, filename):
    if not os.path.exists(filename):
        print("File not found")
        return
    filesize = os.path.getsize(filename)
    # Отправка команды
    sock.sendall(f"UPLOAD {filename} {filesize}\n".encode())
    
    # Ждем подтверждения
    resp = sock.recv(1024).decode()
    if "READY" not in resp:
        print(f"Server refused: {resp}")
        return

    print("Sending data...")
    with open(filename, 'rb') as f:
        data = f.read(4096)
        while data:
            sock.sendall(data)
            data = f.read(4096)
    
    # Ждем отчета о битрейте
    print(sock.recv(1024).decode())

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((HOST, PORT))
        print(f"Connected to {HOST}:{PORT}")
        print("Commands: ECHO <txt>, TIME, UPLOAD <file>, DOWNLOAD <file>, CLOSE")
        
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

            # Обычные команды
            s.sendall((cmd + "\n").encode())
            
            if cmd.strip().upper() == 'CLOSE':
                break
                
            # Чтение ответа
            data = s.recv(4096)
            print("Server:", data.decode(errors='ignore').strip())
            
    except ConnectionRefusedError:
        print("Connection failed. Is server running?")
    finally:
        s.close()

if __name__ == '__main__':
    main()