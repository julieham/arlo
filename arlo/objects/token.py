invalid_token = ''


class Token:

    def __init__(self, value):
        self.value = value

    @staticmethod
    def is_invalid(self):
        return self.value is invalid_token


class ValidToken(Token):
    pass


class InvalidToken(Token):

    def __init__(self):
        Token.__init__(self, invalid_token)
