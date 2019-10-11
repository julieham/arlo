invalid_token = ''


class Token:

    def __init__(self, value):
        self.value = value

    def is_invalid(self):
        return self.value == invalid_token


class InvalidToken(Token):

    def __init__(self):
        Token.__init__(self, invalid_token)
