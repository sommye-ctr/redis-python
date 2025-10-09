import asyncio
from argparse import ArgumentParser

from app.protocol_parser import Protocol


async def main():
    parser = ArgumentParser()
    parser.add_argument("--port", type=int, default=6379, help="Set port for server")

    args = parser.parse_args()

    protocol = Protocol(port=args.port)
    await protocol.start_listening()


if __name__ == "__main__":
    asyncio.run(main())
