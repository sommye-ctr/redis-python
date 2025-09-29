DOLLAR = '$'
ASTERISK = '*'
PLUS = "+"
COLON = ":"
CRLF = '\r\n'

ECHO_CMD = "ECHO"
PING_CMD = "PING"
SET_CMD = "SET"
GET_CMD = "GET"
RPUSH_CMD = "RPUSH"
LRANGE_CMD = "LRANGE"
LPUSH_CMD = "LPUSH"
LLEN_CMD = "LLEN"
LPOP_CMD = "LPOP"
BLPOP_CMD = "BLPOP"

TYPE_CMD = "TYPE"

NULL_BULK = f"{DOLLAR}-1{CRLF}"
NULL_ARRAY = f"{ASTERISK}-1{CRLF}"

BAD_REQ = f"-ERR bad request{CRLF}"
INVALID_BULK = f"-ERR invalid bulk string{CRLF}"
UNKNOWN_CMD = f"-ERR unknown command{CRLF}"
WRONG_TYPE = f"-WRONGTYPE Operation against a key holding the wrong kind of value"
