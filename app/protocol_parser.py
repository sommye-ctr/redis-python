import concurrent.futures
import socket

from app.storage import Storage
from app.utils import fmt_integers, fmt_bulk_str, fmt_simple, fmt_array
from app.constants import *


class Protocol:
    ECHO_CMD = "ECHO"
    PING_CMD = "PING"
    SET_CMD = "SET"
    GET_CMD = "GET"
    RPUSH_CMD = "RPUSH"
    LRANGE_CMD = "LRANGE"
    LPUSH_CMD = "LPUSH"
    LLEN_CMD = "LLEN"
    LPOP_CMD = "LPOP"

    host = 'localhost'
    port = 6379
    _server_socket = None
    _storage = Storage()

    def __init__(self):
        self._server_socket = socket.create_server((self.host, self.port), reuse_port=True)

    def start_listening(self):
        with concurrent.futures.ThreadPoolExecutor() as e:
            while True:
                conn, _ = self._server_socket.accept()
                e.submit(self._get_request_data, conn)

    def _get_request_data(self, connection: socket):
        while True:
            req = connection.recv(512)
            if not req:
                break
            data = req.decode()
            result = self._parse(data)
            connection.sendall(result)

    def _parse(self, data: str):
        lines = data.split(CRLF)
        if lines[0][0] == ASTERISK:
            # -1 because not including the last element which is an empty string
            return self._handle_bulk_string(lines[1:-1], lines[0][1:])

    def _handle_bulk_string(self, lines: [str], n: str):
        try:
            _ = int(n)
        except ValueError:
            return INVALID_BULK.encode()
        length = len(lines)
        if length == 0:
            return BAD_REQ.encode()

        match lines[1].upper():
            case self.ECHO_CMD:
                res = self._echo(length, lines)
            case self.PING_CMD:
                res = self._ping()
            case self.SET_CMD:
                res = self._set(length, lines)
            case self.GET_CMD:
                res = self._get(length, lines)
            case self.RPUSH_CMD:
                res = self._push(length, lines)
            case self.LRANGE_CMD:
                res = self._lrange(length, lines)
            case self.LPUSH_CMD:
                res = self._push(length, lines, True)
            case self.LLEN_CMD:
                res = self._llen(length, lines)
            case self.LPOP_CMD:
                res = self._lpop(length, lines)
            case _:
                res = UNKNOWN_CMD
        if isinstance(res, bytes):
            return res
        return res.encode('utf-8')

    def _ping(self):
        return fmt_simple("PONG")

    def _echo(self, length: int, lines: list):
        if length < 4:
            return BAD_REQ.encode()
        arg = lines[3]
        return fmt_bulk_str(arg)

    def _set(self, length: int, lines: list):
        meta = {}
        if length < 6:
            return BAD_REQ.encode()
        if length > 6:
            vals = lines[7::2]
            for i in range(0, len(vals), 2):
                key = vals[i].lower()
                v = vals[i + 1]
                try:
                    v = int(v)
                except ValueError:
                    pass
                meta[key] = v
        self._storage.set(lines[3], lines[5], meta)
        return fmt_simple("OK")

    def _get(self, length: int, lines: list):
        if length < 4:
            return BAD_REQ.encode()
        resp = self._storage.get(lines[3])
        if resp is None:
            return NULL_BULK
        return fmt_bulk_str(resp.value)

    def _push(self, length: int, lines: list, left: bool = False):
        if length < 6:
            return BAD_REQ.encode()
        resp = self._storage.push(lines[3], lines[5::2], left=left)
        if resp is None:
            return WRONG_TYPE.encode()
        return fmt_integers(len(resp.value))

    def _lrange(self, length: int, lines: list):
        if length < 8:
            return BAD_REQ.encode()
        try:
            start = int(lines[5])
            end = int(lines[7])
        except ValueError:
            return BAD_REQ.encode()

        resp = self._storage.lrange(lines[3], start, end)
        return fmt_array(resp)

    def _llen(self, length: int, lines: list):
        if length < 4:
            return BAD_REQ.encode()
        return fmt_integers(self._storage.llen(lines[3]))

    def _lpop(self, length: int, lines: list):
        if length < 4:
            return BAD_REQ.encode()
        qnt = 1
        if length > 4:
            try:
                qnt = int(lines[5])
            except ValueError:
                return BAD_REQ.encode()

        elements = self._storage.lpop(lines[3], qnt)
        if elements is None:
            return WRONG_TYPE.encode()
        if len(elements) == 0:
            return NULL_BULK
        if len(elements) == 1:
            return fmt_bulk_str(elements[0])
        return fmt_array(elements)
