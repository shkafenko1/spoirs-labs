# 2 ЛАБА
## Запуск UDP сервера:
```bash
python server.py --host 127.0.0.1 --port 6969 --dir ./server_files --protocol udp
```
## Запуск TCP сервера (в другом терминале, на другом порту или после остановки первого):
```bash
python server.py --host 127.0.0.1 --port 6969 --dir ./server_files --protocol tcp
```
## Тест UDP клиента:
```bash
python client.py --host 127.0.0.1 --port 6969 --protocol udp upload bigfile.iso remote.iso
```
## Тест TCP клиента:
```bash
python client.py --host 127.0.0.1 --port 6969 --protocol tcp upload bigfile.iso remote.iso
```