from datetime import datetime, timedelta


class Variable:
    value = None
    meta = {}
    expiry = None

    def __init__(self, value, **kwargs):
        self.value = value
        self.meta = kwargs
        self._add_expiry()

    def _add_expiry(self):
        if self.meta.get('px'):
            self.expiry = datetime.now() + timedelta(milliseconds=self.meta.get('px'))
        elif self.meta.get('ex'):
            self.expiry = datetime.now() + timedelta(seconds=self.meta.get('ex'))
        elif self.meta.get('exat'):
            self.expiry = datetime.fromtimestamp(self.meta.get('exat'))

    def __str__(self):
        return self.value

    def __repr__(self):
        return str(self)
