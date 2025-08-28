import concurrent.futures
import socket  # noqa: F401


def send_pong(conn: socket):
    while True:
        req = conn.recv(512)
        if not req:
            break
        data = req.decode()
        if "ping" in data.lower():
            conn.sendall(b"+PONG\r\n")


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    with concurrent.futures.ThreadPoolExecutor() as e:
        while True:
            conn, _ = server_socket.accept()
            e.submit(send_pong, conn)


if __name__ == "__main__":
    main()
