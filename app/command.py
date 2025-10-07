import inspect
from collections import deque

from app.constants import BAD_REQ, WRONG_TYPE, NULL_BULK, ECHO_CMD, PING_CMD, SET_CMD, GET_CMD, RPUSH_CMD, LRANGE_CMD, \
    LLEN_CMD, LPOP_CMD, LPUSH_CMD, BLPOP_CMD, NULL_ARRAY, TYPE_CMD, INCR_CMD, NOT_INTEGER, MULTI_CMD
from app.errors import WrongTypeError, UndefinedCommandError
from app.storage import Storage
from app.utils import fmt_integer, fmt_bulk_str, fmt_simple, fmt_array


class Command:
    def __init__(self, storage: Storage, requests: list[list]):
        self._storage = storage
        self._requests = requests
        self._commands = {
            ECHO_CMD: self._echo,
            PING_CMD: self._ping,
            SET_CMD: self._set,
            GET_CMD: self._get,
            LRANGE_CMD: self._lrange,
            LLEN_CMD: self._llen,
            LPOP_CMD: self._lpop,
            RPUSH_CMD: self._rpush,
            LPUSH_CMD: self._lpush,
            BLPOP_CMD: self._blpop,
            TYPE_CMD: self._type,
            INCR_CMD: self._incr,
            MULTI_CMD: self._multi,
        }

    async def parse(self):
        resp = bytearray()
        for i in self._requests:
            f = self._commands.get(i[0])
            if f is None:
                raise UndefinedCommandError
            out = f(i[1:])
            if inspect.isawaitable(out):
                out = await out
            resp.extend(out)
        return resp

    def _ping(self, _: list):
        return fmt_simple("PONG")

    def _echo(self, args: list):
        if len(args) == 0:
            return BAD_REQ.encode()
        return fmt_bulk_str(args[0])

    def _set(self, args: list):
        meta = {}
        if len(args) < 2:
            return BAD_REQ.encode()
        if len(args) > 2:
            for i in range(2, len(args), 2):
                key = args[i].lower()
                v = args[i + 1]
                try:
                    v = int(v)
                except ValueError:
                    continue
                meta[key] = v
        self._storage.set(args[0], args[1], meta)
        return fmt_simple("OK")

    def _get(self, args: list):
        if len(args) < 1:
            return BAD_REQ.encode()
        resp = self._storage.get(args[0])
        if resp is None:
            return NULL_BULK.encode()
        return fmt_bulk_str(resp.value)

    def _rpush(self, args: list):
        if len(args) < 2:
            return BAD_REQ.encode()
        try:
            resp = self._storage.push(args[0], args[1:], left=False)
        except WrongTypeError:
            return WRONG_TYPE.encode()
        return fmt_integer(resp)

    def _lpush(self, args: list):
        if len(args) < 2:
            return BAD_REQ.encode()
        try:
            resp = self._storage.push(args[0], args[1:], left=True)
        except WrongTypeError:
            return WRONG_TYPE.encode()
        return fmt_integer(resp)

    def _lrange(self, args: list):
        if len(args) < 3:
            return BAD_REQ.encode()
        try:
            start = int(args[1])
            end = int(args[2])
        except ValueError:
            return BAD_REQ.encode()

        resp = self._storage.lrange(args[0], start, end)
        return fmt_array(resp)

    def _llen(self, args: list):
        if len(args) < 1:
            return BAD_REQ.encode()
        return fmt_integer(self._storage.llen(args[0]))

    def _lpop(self, args: list):
        if len(args) < 1:
            return BAD_REQ.encode()
        qnt = 1
        if len(args) > 1:
            try:
                qnt = int(args[1])
            except ValueError:
                return BAD_REQ.encode()
        try:
            elements = self._storage.lpop(args[0], qnt)
        except WrongTypeError:
            return WRONG_TYPE.encode()
        if len(elements) == 0:
            return NULL_BULK
        if len(elements) == 1:
            return fmt_bulk_str(elements[0])
        return fmt_array(elements)

    async def _blpop(self, args: list):
        if len(args) < 2:
            return BAD_REQ.encode()
        try:
            timeout = float(args[1])
        except ValueError:
            return BAD_REQ.encode()
        resp = await self._storage.blpop(args[0], timeout)
        if resp is None:
            return NULL_ARRAY.encode()
        return fmt_array([args[0], resp])

    def _type(self, args: list):
        if len(args) < 1:
            return BAD_REQ.encode()

        resp = self._storage.get(args[0])
        if resp is None:
            return fmt_simple("none")
        elif isinstance(resp.value, str):
            return fmt_simple("string")
        elif isinstance(resp.value, deque):
            return fmt_simple("list")

    def _incr(self, args: list):
        if len(args) < 1:
            return BAD_REQ.encode()

        var = self._storage.get(args[0])
        if var is None:
            self._storage.set(args[0], 1, {})
            return fmt_integer(1)
        try:
            val = int(var.value)
        except ValueError:
            return NOT_INTEGER.encode()
        var.value = val + 1
        return fmt_integer(val + 1)

    def _multi(self, args: list):
        return fmt_simple("OK")
