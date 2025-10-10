import inspect
from collections import deque
from typing import Optional

from app.constants import BAD_REQ, WRONG_TYPE, NULL_BULK, ECHO_CMD, PING_CMD, SET_CMD, GET_CMD, RPUSH_CMD, LRANGE_CMD, \
    LLEN_CMD, LPOP_CMD, LPUSH_CMD, BLPOP_CMD, NULL_ARRAY, TYPE_CMD, INCR_CMD, NOT_INTEGER, MULTI_CMD, EXEC_CMD, \
    EXEC_WO_MULTI, DISCARD_CMD, DISCARD_WO_MULTI, INFO_CMD
from app.errors import WrongTypeError, UndefinedCommandError
from app.storage import Storage
from app.utils import fmt_integer, fmt_bulk_str, fmt_simple, fmt_array


class Command:
    _transactions = {}

    def __init__(self, storage: Storage,
                 requests: list[list],
                 peer: tuple = None,
                 is_master: Optional[bool] = None):
        self._storage = storage
        self._requests = requests
        self._peer = peer
        self._is_master = is_master
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
            INFO_CMD: self._info,
        }

    async def execute(self):
        cmd = self._requests[0][0].upper()

        if cmd == MULTI_CMD:
            self._transactions[self._peer] = []
            self._transactions[self._peer].extend(self._requests[1:])
            return fmt_simple("OK")

        if cmd == EXEC_CMD:
            if self._peer not in self._transactions:
                return EXEC_WO_MULTI.encode()
            queued = self._transactions.pop(self._peer)
            results = [await self._execute_single(r) for r in queued]
            return fmt_array(results, alr_formatted=True)

        if cmd == DISCARD_CMD:
            if self._peer not in self._transactions:
                return DISCARD_WO_MULTI.encode()
            self._transactions.pop(self._peer)
            return fmt_simple("OK")

        if self._peer in self._transactions:
            self._transactions[self._peer].extend(self._requests)
            return fmt_simple("QUEUED")

        return await self._execute_requests(self._requests)

    async def _execute_requests(self, requests: list):
        resp = bytearray()
        for i in requests:
            ans = await self._execute_single(i)
            resp.extend(ans)
        return resp

    async def _execute_single(self, request: list):
        f = self._commands.get(request[0])
        if f is None:
            raise UndefinedCommandError
        out = f(request[1:])
        if inspect.isawaitable(out):
            out = await out
        return out

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

    def _info(self, args: list):
        val = f"role:{'master' if self._is_master else 'slave'}"
        return fmt_bulk_str(val)
