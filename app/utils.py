from datetime import datetime
from constants import *


def time_now():
    return datetime.now()


def from_ts(s: float):
    return datetime.fromtimestamp(s)


def fmt_simple(s: str) -> bytes:
    return f"{PLUS}{s}{CRLF}".encode()


def fmt_integers(n: int) -> bytes:
    return f"{COLON}{n}{CRLF}".encode()


def fmt_bulk_str(s: any) -> bytes:
    return f"{DOLLAR}{len(s)}{CRLF}{s}{CRLF}".encode()


def fmt_array(arr: list) -> bytes:
    resp = f"{ASTERISK}{len(arr)}{CRLF}"
    for a in arr:
        if a is int:
            resp += fmt_integers(a)
        else:
            resp += fmt_bulk_str(a)
    return resp.encode()
