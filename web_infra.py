from flask import json


class WebServiceResponse:

    def __init__(self):
        self.status = 'FAILURE'
        self.message = ''
        self.data = json.loads('{}')

    def to_json(self):
        return json.jsonify(status=self.status, message=self.message,data=self.data)