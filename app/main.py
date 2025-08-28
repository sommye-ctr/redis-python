import socket  # noqa: F401


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, _ = server_socket.accept()

    while True:
        req = conn.recv(512)
        data = req.decode()
        if "ping" in data.lower():
            conn.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
