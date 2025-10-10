import asyncio
from argparse import ArgumentParser

from app.protocol_parser import Protocol


async def main():
    parser = ArgumentParser()
    parser.add_argument("--port", type=int, default=6379, help="Set port for server")
    parser.add_argument("--replicaof", type=str, help="Master port and server")
    args = parser.parse_args()

    if args.replicaof:
        m_host, m_port = args.replicaof.split()
        protocol = Protocol(port=args.port, master_port=int(m_port), master_host=m_host, is_master=False)
    else:
        protocol = Protocol(port=args.port)
    await protocol.start_listening()


if __name__ == "__main__":
    asyncio.run(main())
