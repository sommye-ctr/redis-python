import concurrent.futures
import socket

DOLLAR = '$'
ASTERISK = '*'
CRLF = '\r\n'


class Protocol:
    ECHO_COMMAND = "ECHO"
    PING_COMMAND = "PING"

    host = 'localhost'
    port = 6379
    _server_socket = None

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
            print(result)
            connection.sendall(result)

    def _parse(self, data: str):
        lines = data.split(r'\r\n')
        if lines[0][0] == ASTERISK:
            return self._handle_bulk_string(lines[1:], lines[0][1:])

    def _handle_bulk_string(self, lines: [str], n: str):
        print(lines)
        print(n)
        try:
            length = int(n)
        except ValueError:
            return f"-ERR invalid bulk string{CRLF}".encode()

        if len(lines) == 0:
            return f"-ERR bad request{CRLF}".encode()

        match lines[1].upper():
            case self.ECHO_COMMAND:
                arg = lines[3]
                res = f"{DOLLAR}{len(arg)}{CRLF}{arg}{CRLF}"
            case self.PING_COMMAND:
                res = f"+PONG{CRLF}"
            case _:
                res = f"-ERR unknown command{CRLF}"

        return res.encode('utf-8')
