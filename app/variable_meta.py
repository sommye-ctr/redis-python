class Variable:
    value = None
    expiry = None

    def __init__(self, value, expiry = None):
        self.value = value
        self.expiry = expiry

    def __str__(self):
        return self.value

    def __repr__(self):
        return str(self)
