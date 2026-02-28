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
