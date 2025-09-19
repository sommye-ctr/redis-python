DOLLAR = '$'
ASTERISK = '*'
PLUS = "+"
COLON = ":"
CRLF = '\r\n'

NULL_BULK = f"{DOLLAR}-1{CRLF}"

BAD_REQ = f"-ERR bad request{CRLF}"
INVALID_BULK = f"-ERR invalid bulk string{CRLF}"
UNKNOWN_CMD = f"-ERR unknown command{CRLF}"
WRONG_TYPE = f"-WRONGTYPE Operation against a key holding the wrong kind of value"
