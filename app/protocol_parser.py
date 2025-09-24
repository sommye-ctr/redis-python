import asyncio
from asyncio import StreamReader, StreamWriter

from app.command import Command
from app.storage import Storage
from app.resp_parser import parse


class Protocol:
    host = 'localhost'
    port = 6379
    _storage = Storage()

    async def start_listening(self):
        server = await asyncio.start_server(
            self._get_request_data,
            self.host,
            self.port,
            reuse_port=True
        )
        async with server:
            await server.serve_forever()

    async def _get_request_data(self, reader: StreamReader, writer: StreamWriter):
        buffer = bytearray()
        try:
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                try:
                    requests, buffer = parse(buffer, chunk)
                except Exception:
                    try:
                        writer.write(b"-ERR protocol error\r\n")
                        await writer.drain()
                    except Exception:
                        pass
                    break
                result = self._parse_requests(requests)
                writer.write(result)
                await writer.drain()
        except Exception:
            writer.write(b"-ERR protocol error\r\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    def _parse_requests(self, requests: list):
        cmd = Command(self._storage, requests)
        return cmd.parse()
