import argparse
import logging
import os
import selectors
import socket
import sys
from dataclasses import dataclass
from typing import Dict, Optional, Tuple


_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common import protocol


LOG = logging.getLogger("lab3.server")


@dataclass
class TransferInfo:
    direction: str  # 'upload' or 'download'
    filename: str
    total_size: int
    offset: int
    bytes_done: int
    start_t: float
    tmp_path: Optional[str] = None
    fh: Optional[object] = None


@dataclass
class ClientState:
    sock: socket.socket
    addr: Tuple[str, int]
    recv_buf: bytearray
    send_buf: bytearray
    mode: str  # 'cmd', 'upload', 'download'
    transfer: Optional[TransferInfo]


def _format_time() -> str:
    import datetime

    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _enqueue(cs: ClientState, data: bytes) -> None:
    cs.send_buf += data


def _close_client(sel: selectors.BaseSelector, cs: ClientState, partials: Dict[Tuple[str, str], Tuple[str, int, str]]) -> None:
    try:
        sel.unregister(cs.sock)
    except Exception:
        pass

    if cs.transfer and cs.transfer.fh is not None:
        try:
            cs.transfer.fh.close()
        except Exception:
            pass

    try:
        cs.sock.close()
    except Exception:
        pass

    LOG.info("disconnected: %s", cs.addr)


def _try_send(cs: ClientState) -> None:
    if not cs.send_buf:
        return
    try:
        sent = cs.sock.send(cs.send_buf)
    except (BlockingIOError, InterruptedError):
        return
    except OSError:
        raise
    if sent > 0:
        del cs.send_buf[:sent]


def _read_to_buffer(cs: ClientState) -> int:
    try:
        data = cs.sock.recv(65536)
    except (BlockingIOError, InterruptedError):
        return 0
    if data == b"":
        raise ConnectionError("closed")
    cs.recv_buf += data
    return len(data)


def _pop_line(cs: ClientState, max_len: int = 8192) -> Optional[str]:
    nl = cs.recv_buf.find(b"\n")
    if nl == -1:
        if len(cs.recv_buf) > max_len:
            raise protocol.ProtocolError("line too long")
        return None
    raw = bytes(cs.recv_buf[: nl + 1])
    del cs.recv_buf[: nl + 1]
    return raw.decode("utf-8", errors="replace")


def _take_bytes(cs: ClientState, n: int) -> Optional[bytes]:
    if len(cs.recv_buf) < n:
        return None
    out = bytes(cs.recv_buf[:n])
    del cs.recv_buf[:n]
    return out


def _start_upload(
    cs: ClientState,
    root: str,
    allow_overwrite: bool,
    partials: Dict[Tuple[str, str], Tuple[str, int, str]],
    filename: str,
    total_size: int,
) -> None:
    if not protocol.safe_filename(filename):
        _enqueue(cs, protocol.format_err("invalid filename"))
        return
    if total_size < 0:
        _enqueue(cs, protocol.format_err("invalid size"))
        return

    os.makedirs(root, exist_ok=True)
    final_path = protocol.safe_join(root, filename)
    tmp_path = final_path + ".part"
    if os.path.exists(final_path) and not allow_overwrite:
        _enqueue(cs, protocol.format_err("file exists"))
        return

    key = (cs.addr[0], filename)
    offset = 0
    if key in partials:
        direction, st_total, st_tmp = partials[key]
        if direction == "upload" and st_total == total_size and os.path.exists(st_tmp):
            offset = os.path.getsize(st_tmp)

    if offset > total_size:
        offset = 0

    if offset == 0:
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        with open(tmp_path, "wb"):
            pass

    partials[key] = ("upload", total_size, tmp_path)

    ti = TransferInfo(
        direction="upload",
        filename=filename,
        total_size=total_size,
        offset=offset,
        bytes_done=0,
        start_t=protocol.monotonic(),
        tmp_path=tmp_path,
        fh=open(tmp_path, "r+b"),
    )
    ti.fh.seek(offset)
    cs.transfer = ti
    cs.mode = "upload"
    _enqueue(cs, protocol.format_ok("OFFSET", offset))
    LOG.info("upload start: client=%s file=%s size=%d offset=%d", cs.addr, filename, total_size, offset)


def _handle_upload_data(cs: ClientState, root: str, partials: Dict[Tuple[str, str], Tuple[str, int, str]], chunk_limit: int) -> None:
    assert cs.transfer is not None
    ti = cs.transfer
    remaining = ti.total_size - (ti.offset + ti.bytes_done)
    if remaining <= 0:
        return

    take = min(remaining, chunk_limit)
    data = _take_bytes(cs, take)
    if data is None:
        return

    ti.fh.write(data)
    ti.bytes_done += len(data)

    remaining = ti.total_size - (ti.offset + ti.bytes_done)
    if remaining == 0:
        ti.fh.close()
        ti.fh = None
        final_path = protocol.safe_join(root, ti.filename)
        os.replace(ti.tmp_path or (final_path + ".part"), final_path)
        key = (cs.addr[0], ti.filename)
        partials.pop(key, None)
        dur = max(1e-6, protocol.monotonic() - ti.start_t)
        bps = int(ti.bytes_done / dur)
        _enqueue(cs, protocol.format_ok("DONE", ti.bytes_done, f"{dur:.3f}", bps))
        LOG.info("upload complete: client=%s file=%s bytes=%d time=%.3fs bps=%d", cs.addr, ti.filename, ti.bytes_done, dur, bps)
        cs.mode = "cmd"
        cs.transfer = None


def _start_download(
    cs: ClientState,
    root: str,
    partials: Dict[Tuple[str, str], Tuple[str, int, str]],
    filename: str,
    offset: int,
) -> None:
    if not protocol.safe_filename(filename):
        _enqueue(cs, protocol.format_err("invalid filename"))
        return
    if offset < 0:
        _enqueue(cs, protocol.format_err("invalid offset"))
        return

    final_path = protocol.safe_join(root, filename)
    if not os.path.exists(final_path) or not os.path.isfile(final_path):
        _enqueue(cs, protocol.format_err("file not found"))
        return

    total_size = os.path.getsize(final_path)
    offset = min(offset, total_size)
    key = (cs.addr[0], filename)
    partials[key] = ("download", total_size, final_path)

    ti = TransferInfo(
        direction="download",
        filename=filename,
        total_size=total_size,
        offset=offset,
        bytes_done=0,
        start_t=protocol.monotonic(),
        fh=open(final_path, "rb"),
    )
    ti.fh.seek(offset)
    cs.transfer = ti
    cs.mode = "download"
    _enqueue(cs, protocol.format_ok("SIZE", total_size, "OFFSET", offset))
    LOG.info("download start: client=%s file=%s size=%d offset=%d", cs.addr, filename, total_size, offset)


def _pump_download(cs: ClientState, partials: Dict[Tuple[str, str], Tuple[str, int, str]], chunk_limit: int) -> None:
    assert cs.transfer is not None
    ti = cs.transfer
    remaining = ti.total_size - (ti.offset + ti.bytes_done)
    if remaining <= 0:
        return
    if cs.send_buf:
        return

    data = ti.fh.read(min(chunk_limit, remaining))
    if not data:
        remaining = 0
    else:
        ti.bytes_done += len(data)
        _enqueue(cs, data)

    remaining = ti.total_size - (ti.offset + ti.bytes_done)
    if remaining == 0 and not cs.send_buf:
        ti.fh.close()
        ti.fh = None
        key = (cs.addr[0], ti.filename)
        partials.pop(key, None)
        dur = max(1e-6, protocol.monotonic() - ti.start_t)
        bps = int(ti.bytes_done / dur)
        _enqueue(cs, protocol.format_ok("DONE", ti.bytes_done, f"{dur:.3f}", bps))
        LOG.info("download complete: client=%s file=%s bytes=%d time=%.3fs bps=%d", cs.addr, ti.filename, ti.bytes_done, dur, bps)
        cs.mode = "cmd"
        cs.transfer = None


def _handle_command(cs: ClientState, root: str, allow_overwrite: bool, partials: Dict[Tuple[str, str], Tuple[str, int, str]]) -> None:
    line = _pop_line(cs)
    if line is None:
        return
    cmd = protocol.parse_command_line(line)

    if cmd.name in ("CLOSE", "QUIT", "EXIT"):
        _enqueue(cs, protocol.format_ok("BYE"))
        raise ConnectionError("close")

    if cmd.name == "TIME":
        _enqueue(cs, (_format_time() + "\n").encode("utf-8"))
        return

    if cmd.name == "ECHO":
        text = cmd.raw[len(cmd.raw.split()[0]) :].lstrip()
        _enqueue(cs, (text + "\n").encode("utf-8"))
        return

    if cmd.name == "UPLOAD":
        if len(cmd.args) != 2:
            _enqueue(cs, protocol.format_err("usage: UPLOAD <remote_filename> <size>"))
            return
        filename, size_s = cmd.args
        try:
            total_size = int(size_s)
        except ValueError:
            _enqueue(cs, protocol.format_err("invalid size"))
            return
        _start_upload(cs, root, allow_overwrite, partials, filename, total_size)
        return

    if cmd.name == "DOWNLOAD":
        if len(cmd.args) not in (1, 2):
            _enqueue(cs, protocol.format_err("usage: DOWNLOAD <remote_filename> [<offset>]"))
            return
        filename = cmd.args[0]
        try:
            offset = int(cmd.args[1]) if len(cmd.args) == 2 else 0
        except ValueError:
            _enqueue(cs, protocol.format_err("invalid offset"))
            return
        _start_download(cs, root, partials, filename, offset)
        return

    _enqueue(cs, protocol.format_err("unknown command"))


def run_server(host: str, port: int, root: str, allow_overwrite: bool, chunk: int) -> None:
    os.makedirs(root, exist_ok=True)
    sel = selectors.DefaultSelector()
    partials: Dict[Tuple[str, str], Tuple[str, int, str]] = {}
    clients: Dict[socket.socket, ClientState] = {}

    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind((host, port))
    lsock.listen()
    lsock.setblocking(False)
    sel.register(lsock, selectors.EVENT_READ, data=None)

    LOG.info("server started (TCP multiplex) on %s:%d root=%s", host, port, os.path.abspath(root))

    try:
        while True:
            events = sel.select(timeout=1.0)
            for key, mask in events:
                if key.data is None:
                    conn, addr = lsock.accept()
                    conn.setblocking(False)
                    protocol.set_tcp_keepalive(conn)
                    cs = ClientState(
                        sock=conn,
                        addr=addr,
                        recv_buf=bytearray(),
                        send_buf=bytearray(),
                        mode="cmd",
                        transfer=None,
                    )
                    clients[conn] = cs
                    sel.register(conn, selectors.EVENT_READ | selectors.EVENT_WRITE, data=cs)
                    LOG.info("connected: %s", addr)
                    continue

                cs: ClientState = key.data
                try:
                    if mask & selectors.EVENT_READ:
                        _read_to_buffer(cs)

                    if cs.mode == "cmd":
                        while True:
                            before = len(cs.recv_buf)
                            _handle_command(cs, root, allow_overwrite, partials)
                            if len(cs.recv_buf) == before:
                                break
                    elif cs.mode == "upload":
                        _handle_upload_data(cs, root, partials, chunk)
                    elif cs.mode == "download":
                        _pump_download(cs, partials, chunk)

                    if mask & selectors.EVENT_WRITE:
                        _try_send(cs)

                except (ConnectionError, OSError):
                    _close_client(sel, cs, partials)
                    clients.pop(cs.sock, None)

            for cs in list(clients.values()):
                if cs.mode == "download":
                    try:
                        _pump_download(cs, partials, chunk)
                    except OSError:
                        _close_client(sel, cs, partials)
                        clients.pop(cs.sock, None)

    except KeyboardInterrupt:
        LOG.info("server stopping")
    finally:
        for cs in list(clients.values()):
            _close_client(sel, cs, partials)
        try:
            sel.unregister(lsock)
        except Exception:
            pass
        try:
            lsock.close()
        except Exception:
            pass
        try:
            sel.close()
        except Exception:
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Lab3 TCP multiplexed server (selectors)")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--root", default="./storage")
    ap.add_argument("--allow-overwrite", action="store_true")
    ap.add_argument("--chunk", type=int, default=16384)
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    run_server(args.host, args.port, args.root, args.allow_overwrite, args.chunk)


if __name__ == "__main__":
    main()
