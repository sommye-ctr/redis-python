import concurrent.futures
import socket
import threading
from collections import defaultdict

from app.variable_meta import Variable
from datetime import datetime

DOLLAR = '$'
ASTERISK = '*'
PLUS = "+"
CRLF = '\r\n'
NULL_BULK = f"{DOLLAR}-1{CRLF}"

BAD_REQ = f"-ERR bad request{CRLF}"
INVALID_BULK = f"-ERR invalid bulk string{CRLF}"
UNKNOWN_CMD = f"-ERR unknown command{CRLF}"


class Protocol:
    ECHO_CMD = "ECHO"
    PING_CMD = "PING"
    SET_CMD = "SET"
    GET_CMD = "GET"

    host = 'localhost'
    port = 6379
    _server_socket = None
    _data: dict[any, Variable] = {}
    _locks = defaultdict(threading.Lock)

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
            case _:
                res = UNKNOWN_CMD
        return res.encode('utf-8')

    def _ping(self):
        return f"{PLUS}PONG{CRLF}"

    def _echo(self, length: int, lines: []):
        if length < 4:
            return BAD_REQ.encode()
        arg = lines[3]
        return f"{DOLLAR}{len(arg)}{CRLF}{arg}{CRLF}"

    def _set(self, length: int, lines: []):
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

        with self._locks[lines[3]]:
            self._data[lines[3]] = Variable(lines[5], **meta)
        return f"{PLUS}OK{CRLF}"

    def _get(self, length: int, lines: []):
        if length < 4:
            return BAD_REQ.encode()
        with self._locks[lines[3]]:
            val = self._data.get(lines[3])
        if val is None or (val.expiry is not None and datetime.now() >= val.expiry):
            return NULL_BULK
        return f"{DOLLAR}{len(val.value)}{CRLF}{val.value}{CRLF}"
