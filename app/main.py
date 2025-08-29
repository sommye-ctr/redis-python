import socket  # noqa: F401

from app.protocol_parser import Protocol


def main():
    protocol = Protocol()
    protocol.start_listening()


if __name__ == "__main__":
    main()
