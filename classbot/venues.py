import json

import requests

from classbot.users import get_token
from parameters.credentials import classpass_login
from parameters.param import classpass_url


def get_user_bookmarks(name):
    user_id = classpass_login[name]['user_id']
    request_url = classpass_url + '/v1/users/' + user_id + '/bookmarks'
    header_token = {'CP-Authorization': "Token " + get_token(name)}
    header_token['Content-Type'] = 'application/json'
    result = requests.get(request_url, headers=header_token)

    my_venues = []
    for venue in result.json()['bookmarks']:
        name = venue['venue']['name']
        if 'subtitle' in venue['venue'].keys():
            subtitle = venue['venue']['subtitle']
            name += (' • ' + subtitle) * (subtitle != '')
        my_venues.append(dict({"name": name, "id": venue['venue']['id']}))
    return json.dumps(my_venues)
