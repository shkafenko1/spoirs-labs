import argparse
import logging
import os
import socket
import sys
import datetime
from dataclasses import dataclass
from typing import Dict, Tuple

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common import protocol

LOG = logging.getLogger("lab1.server")


@dataclass
class PartialTransfer:
    client_ip: str
    direction: str  # 'upload' or 'download'
    filename: str
    total_size: int
    offset: int
    tmp_path: str


def enable_keepalive(sock: socket.socket) -> None:
    """Конфигурация параметров SO_KEEPALIVE в соответствии с заданием."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if hasattr(socket, 'TCP_KEEPIDLE'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
    if hasattr(socket, 'TCP_KEEPINTVL'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
    if hasattr(socket, 'TCP_KEEPCNT'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)


def _send_line(conn: socket.socket, line: bytes) -> None:
    conn.sendall(line)


def _recv_line(buf: bytearray, conn: socket.socket, max_len: int = 8192) -> str:
    while True:
        nl = buf.find(b"\n")
        if nl != -1:
            raw = bytes(buf[: nl + 1])
            del buf[: nl + 1]
            # Обрабатываем \r\n для поддержки telnet/netcat
            return raw.decode("utf-8", errors="replace").strip("\r\n")

        if len(buf) > max_len:
            raise protocol.ProtocolError("line too long")

        chunk = conn.recv(4096)
        if chunk == b"":
            raise EOFError("Client closed connection")
        buf.extend(chunk)


def _recv_exact(buf: bytearray, conn: socket.socket, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        if buf:
            take = min(len(buf), n - len(out))
            out += buf[:take]
            del buf[:take]
            continue
        chunk = conn.recv(min(65536, n - len(out)))
        if chunk == b"":
            raise EOFError("Client closed connection during transfer")
        out += chunk
    return bytes(out)


def _format_time() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def handle_client(
    conn: socket.socket, addr: Tuple[str, int], root: str,
    allow_overwrite: bool, partials: Dict[Tuple[str, str], PartialTransfer], chunk_size: int,
) -> None:
    client_ip, _client_port = addr
    LOG.info("connected: %s", addr)
    enable_keepalive(conn)

    buf = bytearray()
    while True:
        line = _recv_line(buf, conn)
        
        # Если клиент прислал пустую строку (например просто нажал Enter в telnet)
        if not line:
            continue
            
        cmd = protocol.parse_command_line(line)

        # Команды требуемые по заданию
        if cmd.name in ("CLOSE", "QUIT", "EXIT"):
            _send_line(conn, b"OK BYE\r\n")
            return  # Выход из функции закрывает сокет в serve_forever

        if cmd.name == "TIME":
            _send_line(conn, (_format_time() + "\r\n").encode("utf-8"))
            continue

        if cmd.name == "ECHO":
            text = cmd.raw[len(cmd.name):].lstrip()
            _send_line(conn, (text + "\r\n").encode("utf-8"))
            continue

        if cmd.name == "UPLOAD":
            if len(cmd.args) != 2:
                _send_line(conn, protocol.format_err("usage: UPLOAD <remote_filename> <size>"))
                continue
            filename, size_s = cmd.args
            if not protocol.safe_filename(filename):
                _send_line(conn, protocol.format_err("invalid filename"))
                continue
            try:
                total_size = int(size_s)
            except ValueError:
                _send_line(conn, protocol.format_err("invalid size"))
                continue
            if total_size < 0:
                _send_line(conn, protocol.format_err("invalid size"))
                continue

            os.makedirs(root, exist_ok=True)
            final_path = protocol.safe_join(root, filename)
            tmp_path = final_path + ".part"

            if os.path.exists(final_path) and not allow_overwrite:
                _send_line(conn, protocol.format_err("file exists"))
                continue

            key = (client_ip, filename)
            offset = 0
            if key in partials:
                st = partials[key]
                if st.direction == "upload" and st.total_size == total_size and os.path.exists(st.tmp_path):
                    offset = os.path.getsize(st.tmp_path)

            if offset > total_size:
                offset = 0

            if offset == 0:
                try:
                    os.makedirs(os.path.dirname(final_path), exist_ok=True)
                except OSError:
                    _send_line(conn, protocol.format_err("cannot create directory"))
                    continue

                try:
                    with open(tmp_path, "wb"):
                        pass
                except OSError:
                    _send_line(conn, protocol.format_err("cannot open file"))
                    continue

            partials[key] = PartialTransfer(
                client_ip=client_ip, direction="upload", filename=filename,
                total_size=total_size, offset=offset, tmp_path=tmp_path,
            )

            _send_line(conn, protocol.format_ok("OFFSET", offset))

            remaining = total_size - offset
            start_t = protocol.monotonic()
            bytes_written = 0
            try:
                with open(tmp_path, "r+b") as f:
                    f.seek(offset)
                    while remaining > 0:
                        to_read = min(chunk_size, remaining)
                        data = _recv_exact(buf, conn, to_read)
                        f.write(data)
                        bytes_written += len(data)
                        remaining -= len(data)

                os.replace(tmp_path, final_path)
                end_t = protocol.monotonic()
                dur = max(1e-6, end_t - start_t)
                bps = int(bytes_written / dur)
                _send_line(conn, protocol.format_ok("DONE", bytes_written, f"{dur:.3f}", bps))
                partials.pop(key, None)
                LOG.info("upload complete: client=%s file=%s bytes=%d time=%.3fs bps=%d", addr, filename, bytes_written, dur, bps)
            except (ConnectionError, EOFError, OSError) as e:
                LOG.warning("upload interrupted: client=%s file=%s err=%s", addr, filename, e)
                raise # Выбрасываем наверх, чтобы сбросить порванное соединение
            continue

        if cmd.name == "DOWNLOAD":
            if len(cmd.args) not in (1, 2):
                _send_line(conn, protocol.format_err("usage: DOWNLOAD <remote_filename> [<offset>]"))
                continue
            filename = cmd.args[0]
            if not protocol.safe_filename(filename):
                _send_line(conn, protocol.format_err("invalid filename"))
                continue
            try:
                offset_req = int(cmd.args[1]) if len(cmd.args) == 2 else 0
            except ValueError:
                _send_line(conn, protocol.format_err("invalid offset"))
                continue
            if offset_req < 0:
                _send_line(conn, protocol.format_err("invalid offset"))
                continue

            final_path = protocol.safe_join(root, filename)
            # Требование: Решать проблемы связанные с тем что файла нет не нужно, достаточно вывести сообщение
            if not os.path.exists(final_path) or not os.path.isfile(final_path):
                _send_line(conn, protocol.format_err("file not found"))
                continue

            total_size = os.path.getsize(final_path)
            offset = min(offset_req, total_size)
            key = (client_ip, filename)
            partials[key] = PartialTransfer(
                client_ip=client_ip, direction="download", filename=filename,
                total_size=total_size, offset=offset, tmp_path=final_path,
            )

            _send_line(conn, protocol.format_ok("SIZE", total_size, "OFFSET", offset))

            start_t = protocol.monotonic()
            sent = 0
            try:
                with open(final_path, "rb") as f:
                    f.seek(offset)
                    remaining = total_size - offset
                    while remaining > 0:
                        data = f.read(min(chunk_size, remaining))
                        if not data:
                            break
                        conn.sendall(data)
                        sent += len(data)
                        remaining -= len(data)
                end_t = protocol.monotonic()
                dur = max(1e-6, end_t - start_t)
                bps = int(sent / dur)
                _send_line(conn, protocol.format_ok("DONE", sent, f"{dur:.3f}", bps))
                partials.pop(key, None)
                LOG.info("download complete: client=%s file=%s bytes=%d time=%.3fs bps=%d", addr, filename, sent, dur, bps)
            except (ConnectionError, EOFError, OSError) as e:
                LOG.warning("download interrupted: client=%s file=%s err=%s", addr, filename, e)
                raise # Выбрасываем наверх, чтобы сбросить порванное соединение
            continue

        _send_line(conn, protocol.format_err("unknown command"))


def serve_forever(host: str, port: int, root: str, allow_overwrite: bool, chunk_size: int) -> None:
    os.makedirs(root, exist_ok=True)
    LOG.info("server starting (TCP) on %s:%d root=%s", host, port, os.path.abspath(root))

    partials: Dict[Tuple[str, str], PartialTransfer] = {}

    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind((host, port))
    lsock.listen(1)  # Последовательный сервер
    
    try:
        while True:
            conn, addr = lsock.accept()
            try:
                handle_client(conn, addr, root, allow_overwrite, partials, chunk_size)
            except KeyboardInterrupt:
                raise
            except EOFError:
                LOG.info("client finished normally and disconnected: %s", addr)
            except ConnectionError as e:
                LOG.warning("client disconnected abruptly: %s", addr)
            except Exception:
                LOG.exception("client handler error: %s", addr)
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
                LOG.info("disconnected: %s", addr)

            # Требование: "Если успел подключится другой клиент... сервер имеет полное право удалить файлы"
            # Оставляем partials только для текущего клиента (докачка возможна, если клиент сразу переподключится)
            # Файлы от других IP очищаются (или просто забываются в памяти)
            new_partials = {}
            for k, st in partials.items():
                if st.client_ip == addr[0]:
                    new_partials[k] = st
                else:
                    # Физически удаляем фрагменты файлов прерванных загрузок других клиентов
                    try:
                        if st.direction == 'upload' and os.path.exists(st.tmp_path):
                            os.remove(st.tmp_path)
                    except OSError:
                        pass
            partials = new_partials

    finally:
        try:
            lsock.close()
        except Exception:
            pass
        LOG.info("server stopped")


def main() -> None:
    ap = argparse.ArgumentParser(description="Lab1 TCP command + file transfer server (sequential)")
    ap.add_argument("--host", default="0.0.0.0") # Оставьте 0.0.0.0 для принятия подключений извне
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--root", default="./storage")
    ap.add_argument("--allow-overwrite", action="store_true")
    ap.add_argument("--chunk", type=int, default=65536)
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    serve_forever(args.host, args.port, args.root, args.allow_overwrite, args.chunk)

if __name__ == "__main__":
    main()