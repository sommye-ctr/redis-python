from datetime import datetime
from app.constants import *


def time_now():
    return datetime.now()


def from_ts(s: float):
    return datetime.fromtimestamp(s)


def fmt_simple(s: str) -> bytes:
    return f"{PLUS}{s}{CRLF}".encode()


def fmt_integer(n: int) -> bytes:
    return f"{COLON}{n}{CRLF}".encode()


def fmt_bulk_str(s: any) -> bytes:
    b = s if isinstance(s, (bytes, bytearray)) else str(s).encode('utf-8')
    return b"$" + str(len(b)).encode() + CRLF.encode() + b + CRLF.encode()


def fmt_array(arr: list) -> bytes:
    resp = [f"{ASTERISK}{len(arr)}{CRLF}".encode()]
    for a in arr:
        resp.append(fmt_integer(a) if isinstance(a, int) else fmt_bulk_str(a))
    return b"".join(resp)
