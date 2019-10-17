class TwoFactorsAuthError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class N26TokenError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
