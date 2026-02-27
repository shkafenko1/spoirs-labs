# 2 Лаба
## Запуск сервера:
```bash
python server.py --host 127.0.0.1 --port 6969 --dir ./server_files
```
## Загрузка файла (Upload):
```bash
python client.py --host 127.0.0.1 --port 6969 upload my_big_file.iso uploaded_file.iso
```
Вы увидите скорость в Mbps в конце.
## Скачивание файла (Download):
```bash
python client.py --host 127.0.0.1 --port 6969 download uploaded_file.iso my_copy.iso
```