import concurrent.futures
import socket

DOLLAR = '$'
ASTERISK = '*'
PLUS = "+"
CRLF = '\r\n'

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
    _data = {}

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
            return self._handle_bulk_string(lines[1:], lines[0][1:])

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
                if length < 4:
                    return BAD_REQ.encode()
                arg = lines[3]
                res = f"{DOLLAR}{len(arg)}{CRLF}{arg}{CRLF}"
            case self.PING_CMD:
                res = f"+PONG{CRLF}"
            case self.SET_CMD:
                if length < 6:
                    return BAD_REQ.encode()
                self._data[lines[3]] = lines[5]
                res = f"{PLUS}OK{CRLF}"
            case self.GET_CMD:
                if length < 4:
                    return BAD_REQ.encode()
                val = self._data.get(lines[3])
                res = f"{DOLLAR}{len(val)}{CRLF}{val}{CRLF}" if val is not None else f"{DOLLAR}-1{CRLF}"
            case _:
                res = UNKNOWN_CMD

        return res.encode('utf-8')
