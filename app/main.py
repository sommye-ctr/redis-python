import asyncio
from app.protocol_parser import Protocol


async def main():
    protocol = Protocol()
    await protocol.start_listening()


if __name__ == "__main__":
    asyncio.run(main())
