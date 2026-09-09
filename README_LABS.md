# Лабораторные работы по сетевому программированию (TCP)

## Обзор

В данном репозитории реализованы две лабораторные работы по курсу сетевого программирования (Python 3, Linux) **строго на TCP**:

- **Лабораторная работа №1**
  - Последовательный (sequential) TCP-сервер: **один клиент за раз**, один поток.
  - Текстовые команды `ECHO`, `TIME`, `CLOSE`.
  - Передача файлов по той же TCP-сессии: `UPLOAD`, `DOWNLOAD`.
  - Включён TCP keepalive, поддержка возобновления (resume) через `.part`.

- **Лабораторная работа №3**
  - Мультиплексированный TCP-сервер (один поток) на `selectors`.
  - Одновременная работа с несколькими клиентами: команды и передачи файлов **перемежаются**.
  - Совместим с клиентом из ЛР №1 (протокол одинаковый).

Общие функции протокола (парсинг команд, keepalive, безопасность путей) находятся в `common/protocol.py`.

---

## Протокол команд

Команды текстовые, заканчиваются `\n` или `\r\n`.

- `ECHO <текст>`
  - Ответ: `<текст>\n`

- `TIME`
  - Ответ: `YYYY-mm-dd HH:MM:SS\n`

- `CLOSE` / `QUIT` / `EXIT`
  - Ответ: `OK BYE\n`, затем разрыв соединения

### Команды передачи файлов

- `UPLOAD <remote_filename> <size>`
  - Сервер отвечает: `OK OFFSET <n>\n` (смещение для resume)
  - Затем клиент отправляет **ровно** `<size - n>` байт данных
  - Сервер завершает: `OK DONE <bytes> <seconds> <bps>\n`

- `DOWNLOAD <remote_filename> [<offset>]`
  - Сервер отвечает: `OK SIZE <size> OFFSET <n>\n`
  - Затем сервер отправляет **ровно** `<size - n>` байт данных
  - Сервер завершает: `OK DONE <bytes> <seconds> <bps>\n`

---

## Лабораторная работа №1 (последовательный TCP-сервер)

### Запуск сервера

```bash
python3 lab1/server.py --host 0.0.0.0 --port 9000 --root ./lab1/storage
```

Опции:

- `--allow-overwrite` — разрешить перезапись существующих файлов
- `--chunk <bytes>` — размер блока чтения/записи при передаче файлов (по умолчанию `65536`)

### Проверка через telnet / netcat

```bash
nc 127.0.0.1 9000
TIME
ECHO hello world
CLOSE
```

### Демонстрация (nmap / ss)

```bash
nmap -p 9000 127.0.0.1
ss -lntp | grep 9000
```

### Запуск клиента (свой клиент)

Простые команды:

```bash
python3 lab1/client.py --host 127.0.0.1 --port 9000 send "TIME"
python3 lab1/client.py --host 127.0.0.1 --port 9000 send "ECHO hello"
```

Загрузка файла на сервер:

```bash
python3 lab1/client.py --host 127.0.0.1 --port 9000 upload ./local.txt remote.txt
```

Скачивание файла с сервера:

```bash
python3 lab1/client.py --host 127.0.0.1 --port 9000 download remote.txt ./downloaded.txt
```

Режим восстановления (resume) делается автоматически через временный файл `*.part`.

---

## Лабораторная работа №3 (мультиплексирование, один поток)

### Запуск сервера

```bash
python3 lab3/server.py --host 0.0.0.0 --port 9000 --root ./lab3/storage
```

Опции:

- `--allow-overwrite` — разрешить перезапись существующих файлов
- `--chunk <bytes>` — максимальный кусок на один шаг event loop (по умолчанию `16384`)

### Клиент

Клиент совместим с ЛР №1:

```bash
python3 lab3/client.py --host 127.0.0.1 --port 9000 send "TIME"
python3 lab3/client.py --host 127.0.0.1 --port 9000 upload ./local.bin remote.bin
python3 lab3/client.py --host 127.0.0.1 --port 9000 download remote.bin ./copy.bin
```

---

## Структура файлов

```
.
├── common/
│   ├── __init__.py
│   └── protocol.py
├── lab1/
│   ├── server.py
│   └── client.py
├── lab3/
│   ├── server.py
│   └── client.py
└── README_LABS.md
```

---

## Требования

- Linux
- Python 3
- Только стандартная библиотека (`socket`, `selectors`, `argparse`, `logging`, `os`, `time`, `pathlib` и т.д.)

---

## Лабораторные работы №5–8

Подробные инструкции — в `labN/labN.md`. Кратко:

### ЛР №5 — ICMP/IP: параллельный ping, traceroute, Smurf

```bash
sudo python3 lab5/ping.py 8.8.8.8 1.1.1.1 ya.ru      # параллельный ping (поток на хост, MSG_PEEK)
sudo python3 lab5/ping.py --trace 8.8.8.8            # traceroute (рост TTL)
sudo python3 lab5/smurf.py --victim 192.168.1.50 --broadcast 192.168.1.255
```

Raw-сокеты требуют прав root/administrator. Только стандартная библиотека.

### ЛР №6 — broadcast + multicast: одноранговый чат

```bash
python3 lab6/chat.py --nick alice
```

Команды: `/who`, `/net`, `/nick`, `/leave`, `/join`, `/ignore <ip>`, `/quit`.
Автоопределение IP/маски/broadcast, обнаружение участников, выход из группы,
игнорирование хостов. Только стандартная библиотека.

### ЛР №7 — MPI: парные операции (блокирующие/неблокирующие)

```bash
pip install -r requirements.txt          # numpy, mpi4py (нужна установленная MPI)
mpirun -np 4 python3 lab7/matmul.py --size 1500 --mode both
```

### ЛР №8 — MPI: группы, коллективы, файловый ввод-вывод

```bash
python3 lab8/matmul_groups.py --make-input --size 1200 --dir ./shared
mpirun --oversubscribe -np 8 python3 lab8/matmul_groups.py --size 1200 --groups 3 --dir ./shared
```

## Установка MPI (для ЛР №7–8)

```bash
# macOS
brew install open-mpi
# Debian/Ubuntu
sudo apt install -y openmpi-bin libopenmpi-dev
# затем
pip install -r requirements.txt
```
