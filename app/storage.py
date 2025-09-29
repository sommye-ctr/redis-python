import asyncio
from asyncio import Future, InvalidStateError
from collections import deque, defaultdict
from datetime import timedelta

from app.errors import WrongTypeError
from app.utils import time_now, from_ts
from app.variable_meta import Variable


class Storage:

    def __init__(self):
        self._data: dict[any, Variable] = {}
        self._waiters = defaultdict(deque)

    def set(self, key: str, val: any, meta: dict):
        expiry = None
        if meta.get('px'):
            expiry = time_now() + timedelta(milliseconds=meta.get('px'))
        elif meta.get('ex'):
            expiry = time_now() + timedelta(seconds=meta.get('ex'))
        elif meta.get('exat'):
            expiry = from_ts(meta.get('exat'))
        self._data[key] = Variable(val, expiry)

    def get(self, key: str) -> Variable | None:
        val = self._data.get(key)
        if val is None:
            return None
        elif val.expiry is not None and time_now() >= val.expiry:
            del self._data[key]
            return None
        return val

    def _islist(self, key: str) -> bool:
        var = self._data.get(key)
        if var is None:
            return False
        elif not isinstance(var.value, deque):
            return False
        return True

    def push(self, key: str, vals: list, left: bool = False) -> int:
        var: Variable = self._data.get(key)
        if var is not None and not self._islist(key):
            raise WrongTypeError

        iter_vals = reversed(vals) if left else vals
        remaining = []
        length = len(vals)

        for v in iter_vals:
            done = False
            while self._waiters[key]:
                future: Future = self._waiters[key].popleft()
                if future.done():
                    continue
                try:
                    future.set_result(v)
                    done = True
                    break
                except InvalidStateError:
                    continue
            if not done:
                remaining.append(v)

        if var is None:
            self._data[key] = Variable(value=deque(remaining))
        else:
            length += len(var.value)
            if left:
                for i in reversed(remaining):
                    var.value.appendleft(i)
            else:
                var.value.extend(vals)
        return length

    def lrange(self, key: str, start: int, end: int) -> list:
        var = self._data.get(key)
        if var is None:
            return []
        n = len(var.value)

        if start < 0:
            start = max(0, n + start)
        if end < 0:
            end = max(0, n + end)
        if start >= n or start > end:
            return []
        end = min(end, n - 1)
        return [var.value[x] for x in range(start, end + 1)]

    def llen(self, key: str) -> int:
        var = self._data.get(key)
        return 0 if var is None else len(var.value)

    def lpop(self, key: str, count) -> list:
        var = self._data.get(key)
        if var is None:
            return []
        if not self._islist(key):
            raise WrongTypeError
        popped = []
        for _ in range(min(count, len(var.value))):
            popped.append(var.value.popleft())
        return popped

    async def blpop(self, key: str, timeout):
        if self._data.get(key) and self._islist(key):
            return self._data.get(key).value.popleft()

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._waiters[key].append(future)

        try:
            if timeout is None or timeout == 0:
                return await future
            else:
                return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            try:
                self._waiters[key].remove(future)
            except ValueError:
                pass
