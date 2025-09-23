import concurrent.futures
import socket

from app.command import Command
from app.storage import Storage
from app.resp_parser import parse


class Protocol:
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
        buffer = bytearray()
        while True:
            chunk = connection.recv(4096)
            if not chunk:
                break
            try:
                requests, buffer = parse(buffer, chunk)
            except Exception:
                try:
                    connection.sendall(b"-ERR protocol error\r\n")
                except Exception:
                    pass
                break
            result = self._parse(requests)
            connection.sendall(result)

    def _parse(self, requests: list):
        cmd = Command(self._storage, requests)
        return cmd.parse()
