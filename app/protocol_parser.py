import asyncio
from asyncio import StreamReader, StreamWriter

from app.command import Command
from app.resp_parser import parse
from app.storage import Storage


class Protocol:
    host = 'localhost'
    _storage = Storage()

    def __init__(self, port: int = 6379):
        self.port = port

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
        peer = writer.get_extra_info("peername")

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
                result = await self._parse_requests(requests, peer)
                writer.write(result)
                await writer.drain()
        except Exception:
            writer.write(b"-ERR protocol error\r\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def _parse_requests(self, requests: list, peer: tuple):
        cmd = Command(self._storage, requests, peer)
        return await cmd.execute()
