import argparse
import logging
import os
import socket
import sys
import time
from pathlib import Path
from typing import Tuple

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common import protocol

LOG = logging.getLogger("lab1.client")


def enable_keepalive(sock: socket.socket) -> None:
    """Конфигурация параметров SO_KEEPALIVE в соответствии с заданием."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    
    # Настройки специфичные для Linux/Unix для точного контроля интервала
    if hasattr(socket, 'TCP_KEEPIDLE'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)  # Ждать 30с перед отправкой
    if hasattr(socket, 'TCP_KEEPINTVL'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10) # Интервал между пакетами 10с
    if hasattr(socket, 'TCP_KEEPCNT'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)    # 5 пакетов до признания разрыва


def _connect(host: str, port: int, timeout_s: float) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout_s)
    s.connect((host, port))
    enable_keepalive(s)
    return s


def _recv_line(sock: socket.socket, buf: bytearray, timeout_s: float) -> str:
    sock.settimeout(timeout_s)
    while True:
        nl = buf.find(b"\n")
        if nl != -1:
            raw = bytes(buf[: nl + 1])
            del buf[: nl + 1]
            return raw.decode("utf-8", errors="replace").strip("\r\n")
        chunk = sock.recv(4096)
        if chunk == b"":
            raise EOFError("connection closed by server")
        buf.extend(chunk)


def _recv_exact(sock: socket.socket, buf: bytearray, n: int, timeout_s: float) -> bytes:
    sock.settimeout(timeout_s)
    out = bytearray()
    while len(out) < n:
        if buf:
            take = min(len(buf), n - len(out))
            out += buf[:take]
            del buf[:take]
            continue
        chunk = sock.recv(min(65536, n - len(out)))
        if chunk == b"":
            raise EOFError("connection closed by server")
        out += chunk
    return bytes(out)


def _send_line(sock: socket.socket, line: str) -> None:
    sock.sendall(line.encode("utf-8"))


def _parse_ok_offset(line: str) -> int:
    cmd = protocol.parse_command_line(line)
    if cmd.name != "OK":
        raise protocol.ProtocolError(f"expected OK, got: {line}")
    parts = cmd.args
    if len(parts) != 2 or parts[0].upper() != "OFFSET":
        raise protocol.ProtocolError("expected OK OFFSET <n>")
    return int(parts[1])


def _parse_ok_size_offset(line: str) -> Tuple[int, int]:
    cmd = protocol.parse_command_line(line)
    if cmd.name != "OK":
        raise protocol.ProtocolError(f"expected OK, got: {line}")
    parts = [p for p in cmd.args]
    if len(parts) != 4:
        raise protocol.ProtocolError("expected OK SIZE <n> OFFSET <m>")
    if parts[0].upper() != "SIZE" or parts[2].upper() != "OFFSET":
        raise protocol.ProtocolError("expected OK SIZE <n> OFFSET <m>")
    return int(parts[1]), int(parts[3])


def _is_err(line: str) -> bool:
    try:
        cmd = protocol.parse_command_line(line)
    except Exception:
        return False
    return cmd.name == "ERR"


def _prompt_retry(error_msg: str) -> bool:
    sys.stderr.write(f"\n[NETWORK PROBLEM] Соединение прервано: {error_msg}\n")
    try:
        ans = input("Попробовать восстановить передачу? [y/N]: ").strip().lower()
    except EOFError:
        return False
    return ans in ("y", "yes")


def upload_file(
    host: str, port: int, local_path: Path, remote_filename: str,
    timeout_s: float, progress_timeout_s: float, max_auto_reconnect: int, non_interactive: bool
) -> int:
    size = local_path.stat().st_size
    attempts = 0

    while True:
        buf = bytearray()
        try:
            sock = _connect(host, port, timeout_s)
            try:
                _send_line(sock, f"UPLOAD {remote_filename} {size}\r\n")
                line = _recv_line(sock, buf, timeout_s)
                if _is_err(line):
                    sys.stderr.write(line + "\n")
                    return 1
                offset = _parse_ok_offset(line)

                start_t = protocol.monotonic()
                last_progress = start_t
                sent = 0

                with local_path.open("rb") as f:
                    f.seek(offset)
                    remaining = size - offset
                    while remaining > 0:
                        chunk = f.read(min(65536, remaining))
                        if not chunk:
                            raise OSError("unexpected EOF in local file")
                        sock.sendall(chunk)
                        sent += len(chunk)
                        remaining -= len(chunk)
                        last_progress = protocol.monotonic()

                        if protocol.monotonic() - last_progress > progress_timeout_s:
                            raise TimeoutError("no progress timeout")

                done_line = _recv_line(sock, buf, timeout_s)
                if _is_err(done_line):
                    sys.stderr.write(done_line + "\n")
                    return 1
                end_t = protocol.monotonic()
                dur = max(1e-6, end_t - start_t)
                bps = int(sent / dur)
                
                LOG.info("upload done: file=%s bytes=%d time=%.3fs bps=%d", remote_filename, sent, dur, bps)
                print(f"Успешно загружено. Скорость передачи (битрейт): {bps / 1024:.2f} KB/s")
                return 0
            finally:
                try:
                    sock.close()
                except Exception:
                    pass
        except (OSError, ConnectionError, TimeoutError, EOFError) as e:
            attempts += 1
            if attempts <= max_auto_reconnect:
                LOG.info("Автоматическое переподключение (%d/%d)...", attempts, max_auto_reconnect)
                time.sleep(1.0)
                continue
                
            if non_interactive:
                sys.stderr.write(f"Передача не удалась: {e}\n")
                return 1
                
            if _prompt_retry(str(e)):
                attempts = 0
                continue
            
            sys.stderr.write("Передача отменена пользователем.\n")
            return 1


def download_file(
    host: str, port: int, remote_filename: str, local_path: Path,
    timeout_s: float, progress_timeout_s: float, max_auto_reconnect: int, non_interactive: bool
) -> int:
    part_path = local_path.with_suffix(local_path.suffix + ".part")
    attempts = 0

    while True:
        buf = bytearray()
        try:
            sock = _connect(host, port, timeout_s)
            try:
                offset = part_path.stat().st_size if part_path.exists() else 0
                _send_line(sock, f"DOWNLOAD {remote_filename} {offset}\r\n")
                line = _recv_line(sock, buf, timeout_s)
                if _is_err(line):
                    sys.stderr.write(line + "\n")
                    return 1
                
                total_size, offset_server = _parse_ok_size_offset(line)
                if offset_server != offset:
                    offset = offset_server

                start_t = protocol.monotonic()
                last_progress = start_t
                received = 0

                os.makedirs(str(local_path.parent), exist_ok=True)
                with part_path.open("ab") as f:
                    remaining = total_size - offset
                    while remaining > 0:
                        to_read = min(65536, remaining)
                        data = _recv_exact(sock, buf, to_read, timeout_s)
                        f.write(data)
                        received += len(data)
                        remaining -= len(data)
                        last_progress = protocol.monotonic()

                        if protocol.monotonic() - last_progress > progress_timeout_s:
                            raise TimeoutError("no progress timeout")

                done_line = _recv_line(sock, buf, timeout_s)
                if _is_err(done_line):
                    sys.stderr.write(done_line + "\n")
                    return 1

                os.replace(part_path, local_path)
                end_t = protocol.monotonic()
                dur = max(1e-6, end_t - start_t)
                bps = int(received / dur)
                
                LOG.info("download done: file=%s bytes=%d time=%.3fs bps=%d", remote_filename, received, dur, bps)
                print(f"Успешно скачано. Скорость передачи (битрейт): {bps / 1024:.2f} KB/s")
                return 0
            finally:
                try:
                    sock.close()
                except Exception:
                    pass
        except (OSError, ConnectionError, TimeoutError, EOFError) as e:
            attempts += 1
            if attempts <= max_auto_reconnect:
                LOG.info("Автоматическое переподключение (%d/%d)...", attempts, max_auto_reconnect)
                time.sleep(1.0)
                continue
                
            if non_interactive:
                sys.stderr.write(f"Передача не удалась: {e}\n")
                return 1
                
            if _prompt_retry(str(e)):
                attempts = 0
                continue
            
            sys.stderr.write("Передача отменена пользователем.\n")
            return 1


def send_simple_command(host: str, port: int, cmd_line: str, timeout_s: float) -> int:
    buf = bytearray()
    try:
        sock = _connect(host, port, timeout_s)
        try:
            if not cmd_line.endswith("\n"):
                cmd_line += "\r\n"
            _send_line(sock, cmd_line)
            resp = _recv_line(sock, buf, timeout_s)
            sys.stdout.write(resp + "\n")
            return 0
        finally:
            sock.close()
    except OSError as e:
        sys.stderr.write(f"Error: {e}\n")
        return 1


def main() -> None:
    ap = argparse.ArgumentParser(description="Lab1 TCP client (commands + file transfer)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--progress-timeout", type=float, default=120.0)
    ap.add_argument("--max-auto-reconnect", type=int, default=3) # По умолчанию 3 попытки автоматического восстановления
    ap.add_argument("--non-interactive", action="store_true")
    ap.add_argument("--log-level", default="INFO")

    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("send")
    s.add_argument("command", help="raw command line, e.g. 'TIME' or 'ECHO hello'")

    u = sub.add_parser("upload")
    u.add_argument("local_path")
    u.add_argument("remote_filename")

    d = sub.add_parser("download")
    d.add_argument("remote_filename")
    d.add_argument("local_path")

    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.cmd == "send":
        raise SystemExit(send_simple_command(args.host, args.port, args.command, args.timeout))

    if args.cmd == "upload":
        raise SystemExit(
            upload_file(
                args.host, args.port, Path(args.local_path), args.remote_filename,
                args.timeout, args.progress_timeout, args.max_auto_reconnect, args.non_interactive,
            )
        )

    if args.cmd == "download":
        raise SystemExit(
            download_file(
                args.host, args.port, args.remote_filename, Path(args.local_path),
                args.timeout, args.progress_timeout, args.max_auto_reconnect, args.non_interactive,
            )
        )

if __name__ == "__main__":
    main()