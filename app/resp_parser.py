from app.constants import CRLF, DOLLAR


def parse(buffer: bytearray, data: bytes) -> (list, bytearray):
    requests = []
    buffer.extend(data)

    while True:
        if not buffer:
            break

        try:
            end = buffer.index(CRLF.encode())
        except ValueError:
            break

        count = int(buffer[1:end])
        pos = end + 2

        items = []
        for _ in range(count):
            if pos >= len(buffer) or buffer[pos:pos + 1] != DOLLAR.encode():
                return requests, buffer

            try:
                end = buffer.index(CRLF.encode(), pos)
            except ValueError:
                return requests, buffer

            bulk_s_length = int(buffer[pos + 1:end])
            pos = end + 2

            if len(buffer) < pos + bulk_s_length + 2:
                return requests, buffer

            val = buffer[pos:pos + bulk_s_length].decode()
            items.append(val)

            pos += bulk_s_length + 2

        requests.append(items)
        buffer = buffer[pos:]

    return requests, buffer
