"""ЛР №6: автоопределение сетевых параметров интерфейса.

Кроссплатформенно перечисляет интерфейсы (через `ip addr` на Linux или
`ifconfig` на macOS/BSD) и выбирает подходящий для LAN: имеющий
широковещательный адрес, не loopback и не VPN/point-to-point. Возвращает
IP, маску и broadcast. Можно принудительно задать IP интерфейса.
"""
import ipaddress
import socket
import subprocess


def _run(cmd: list) -> str:
    """Запускает команду и возвращает stdout ('' при ошибке/отсутствии)."""
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return ""


def _parse_ifconfig(out: str) -> list:
    """Разбирает вывод ifconfig (macOS/BSD) в список интерфейсов с broadcast."""
    result = []
    for line in out.splitlines():
        s = line.strip()
        if not s.startswith("inet ") or "broadcast" not in s.split():
            continue
        t = s.split()
        ip = t[1]
        mask = t[t.index("netmask") + 1]
        if mask.startswith("0x"):
            mask = socket.inet_ntoa(int(mask, 16).to_bytes(4, "big"))
        result.append({"ip": ip, "netmask": mask, "broadcast": t[t.index("broadcast") + 1]})
    return result


def _parse_ip_addr(out: str) -> list:
    """Разбирает вывод `ip -o -f inet addr` (Linux) в список интерфейсов с brd."""
    result = []
    for line in out.splitlines():
        t = line.split()
        if "inet" not in t or "brd" not in t:
            continue
        ip, prefix = t[t.index("inet") + 1].split("/")
        mask = str(ipaddress.IPv4Network(f"0.0.0.0/{prefix}").netmask)
        result.append({"ip": ip, "netmask": mask, "broadcast": t[t.index("brd") + 1]})
    return result


def list_interfaces() -> list:
    """Список интерфейсов с broadcast (loopback и VPN отфильтрованы отсутствием brd)."""
    ifaces = _parse_ip_addr(_run(["ip", "-o", "-f", "inet", "addr", "show"]))
    if not ifaces:
        ifaces = _parse_ifconfig(_run(["ifconfig"]))
    return [i for i in ifaces if not i["ip"].startswith("127.")]


def _fallback_info() -> dict:
    """Запасной вариант, если утилиты недоступны: IP по маршруту + маска /24."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except OSError:
        ip = "127.0.0.1"
    finally:
        s.close()
    net = ipaddress.IPv4Network(f"{ip}/24", strict=False)
    return {"ip": ip, "netmask": "255.255.255.0", "broadcast": str(net.broadcast_address)}


def interface_info(preferred_ip: str = None) -> dict:
    """Возвращает {ip, netmask, broadcast} выбранного интерфейса.

    preferred_ip — принудительно выбрать интерфейс с этим адресом.
    Иначе берётся первый LAN-интерфейс с broadcast.
    """
    ifaces = list_interfaces()
    if preferred_ip:
        for i in ifaces:
            if i["ip"] == preferred_ip:
                return i
    return ifaces[0] if ifaces else _fallback_info()
