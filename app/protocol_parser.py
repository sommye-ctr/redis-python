import asyncio
from asyncio import StreamReader, StreamWriter
from typing import Optional

from app.command import Command
from app.resp_parser import parse
from app.storage import Storage
from app.utils import gen_master_id, fmt_array


class Protocol:
    host = 'localhost'
    _storage = Storage()

    def __init__(self, port: int = 6379, master_port: Optional[int] = None, master_host: Optional[int | str] = None,
                 is_master: bool = True):
        self.port = port
        self.master_port = master_port
        self.master_host = master_host
        self.is_master = is_master
        self._replicas = [] if self.is_master else None
        self.master_repl_id = gen_master_id()
        self.master_repl_offset = 0

    async def start_listening(self):
        server = await asyncio.start_server(
            self._get_request_data,
            self.host,
            self.port,
            reuse_port=True
        )
        if not self.is_master:
            asyncio.create_task(self._connect_with_master())
        async with server:
            await server.serve_forever()

    async def _connect_with_master(self):
        reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
        message = fmt_array(["PING"])
        writer.write(message)
        await writer.drain()

        writer.close()
        await writer.wait_closed()

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
        cmd = Command(self._storage, requests, peer,
                      is_master=self.is_master,
                      master_id=self.master_repl_id,
                      master_offset=self.master_repl_offset)
        return await cmd.execute()
