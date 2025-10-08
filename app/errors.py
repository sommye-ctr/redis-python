class WrongTypeError(Exception):
    pass

class UndefinedCommandError(Exception):
    def __init__(self):
        super().__init__("Command is not defined!")
