import json
import requests

from operations.date_operations import get_timestamp_now
from parameters.credentials import login_N26
from parameters.param import n26_auth_url, directory_tokens, n26_auth_header
from tools.errors import N26TokenError
from tools.logging import error
from tools.scheduler import pause_scheduler

invalid_token = ''
invalid_name = 'my_name'


class Token:

    def __init__(self, value):
        self.value = value

    def is_invalid(self):
        return self.value == invalid_token


class InvalidToken(Token):

    def __init__(self):
        Token.__init__(self, invalid_token)


class N26_Token:

    def __init__(self, dictionary, name):
        self.expiration = 0
        self.refresh_token = invalid_token
        self.access_token = invalid_token
        self.name = name
        for key, value in dictionary.items():
            if key in ['access_token', 'refresh_token', 'expiration', 'name']:
                setattr(self, key, value)
            if 'expires_in' in dictionary:
                self.expiration = get_timestamp_now() + dictionary['expires_in']*1000

    def update(self, dictionary):
        try:
            for key, value in dictionary.items():
                if key in ['access_token', 'refresh_token', 'expiration', 'name']:
                    setattr(self, key, value)
            self.save()
        except KeyError:
            raise N26TokenError

    def invalidate(self):
        EmptyN26Token.__init__(self, self.name)
        self.save()

    @property
    def is_invalid(self):
        return (self.expiration <= get_timestamp_now()) | (self.access_token == invalid_token)

    @property
    def refresh_token_is_invalid(self):
        return self.refresh_token == invalid_token

    def __str__(self):
        return json.dumps(self.__dict__)

    def refresh(self):
        if self.is_invalid and not self.refresh_token_is_invalid:
            print('refreshing token from ' + self.name)
            values_token = {"grant_type": "refresh_token", 'refresh_token': self.refresh_token}
            response = requests.post(n26_auth_url, data=values_token, headers=n26_auth_header)
            if response.status_code == 429:
                error(' Too many log-in attempts')
            elif response.status_code == 401:
                error('Invalid refresh token for ' + self.name + ', could not login')
                self.update({'refresh_token': invalid_token, 'access_token': invalid_token, 'expiration':0})
                pause_scheduler()
            else:
                try:
                    print(response.content)
                    self.update(json.loads(response.content))
                except N26TokenError as e:
                    error(e)


    def save(self):
        with open(token_filename(self.name), "w") as file:
            file.write(json.dumps(self.__dict__))


class EmptyN26Token(N26_Token):

    def __init__(self):
        N26_Token.__init__(self, dict(), invalid_name)

    def save(self):
        if self.name in login_N26:
            N26_Token.save(self)

    def refresh(self):
        pass

    def __str__(self):
        return N26_Token.__str__(self)


def token_filename(name):
    return directory_tokens + name + '_dict' + '.txt'


def read_token(name):
    if name not in login_N26:
        print('name not found')
        return EmptyN26Token()
    with open(token_filename(name), "r") as file:
        print('reading token from ' + name)
        return N26_Token(json.loads(file.read()), name)


def get_token(name):
    current_token = read_token(name)
    current_token.refresh()
    if current_token.is_invalid:
        return EmptyN26Token
    return current_token
