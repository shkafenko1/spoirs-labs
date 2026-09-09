"""ЛР №6: автоопределение сетевых параметров интерфейса.

Кроссплатформенно определяет локальный IP, сетевую маску и широковещательный
адрес. Маска ищется в выводе системных утилит (`ip addr` на Linux,
`ifconfig` на macOS/BSD) по совпадению с нашим IP, с запасным вариантом /24.
"""
import ipaddress
import socket
import subprocess


def primary_ip() -> str:
    """Определяет основной локальный IP (по маршруту наружу, без реального трафика)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def _run(cmd: list) -> str:
    """Запускает команду и возвращает stdout ('' при ошибке/отсутствии)."""
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return ""


def _mask_from_ip_addr(ip: str) -> str:
    """Маска из `ip -o -f inet addr show` (Linux): формат 'inet IP/PREFIX'."""
    out = _run(["ip", "-o", "-f", "inet", "addr", "show"])
    for line in out.splitlines():
        for token in line.split():
            if token.startswith(ip + "/"):
                prefix = int(token.split("/")[1])
                return str(ipaddress.IPv4Network(f"0.0.0.0/{prefix}").netmask)
    return ""


def _mask_from_ifconfig(ip: str) -> str:
    """Маска из вывода ifconfig (macOS/BSD): 'inet IP netmask 0x...'."""
    out = _run(["ifconfig"])
    for line in out.splitlines():
        line = line.strip()
        if line.startswith("inet ") and ip in line.split():
            return _parse_ifconfig_mask(line)
    return ""


def _parse_ifconfig_mask(line: str) -> str:
    """Достаёт маску из строки ifconfig (hex 0xffffff00 или dotted)."""
    tokens = line.split()
    if "netmask" not in tokens:
        return ""
    mask = tokens[tokens.index("netmask") + 1]
    if mask.startswith("0x"):
        return socket.inet_ntoa(int(mask, 16).to_bytes(4, "big"))
    return mask


def netmask(ip: str) -> str:
    """Определяет маску сети доступным способом, иначе /24."""
    return _mask_from_ip_addr(ip) or _mask_from_ifconfig(ip) or "255.255.255.0"


def broadcast_address(ip: str, mask: str) -> str:
    """Вычисляет широковещательный адрес по IP и маске."""
    net = ipaddress.IPv4Network(f"{ip}/{mask}", strict=False)
    return str(net.broadcast_address)


def interface_info() -> dict:
    """Возвращает словарь: ip, netmask, broadcast."""
    ip = primary_ip()
    mask = netmask(ip)
    return {"ip": ip, "netmask": mask, "broadcast": broadcast_address(ip, mask)}
