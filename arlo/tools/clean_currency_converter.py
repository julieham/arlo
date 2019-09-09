import json

import requests

currency_converter_url = 'https://api.exchangeratesapi.io/latest'


def latest_rate_from_euro(currency_code):
    if currency_code == 'EUR':
        return 1
    rates = json.loads(requests.get(currency_converter_url).content)["rates"]
    return rates[currency_code]
