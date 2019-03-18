success_value = 'SUCCESS'
fail_value = 'FAIL'


def _my_response(success, message=''):
    return dict({'status': success_value if success else fail_value,
                 'message': message})


def success_response():
    return _my_response(True)


def failure_response(message):
    return _my_response(False, message)


def is_successful(response):
    return response['status'] == success_value


def is_fail(response):
    return response['status'] == fail_value


def merge_status(response1, response2):
    if is_successful(response1):
        return response2
    if is_fail(response1) and is_fail(response2):
        return failure_response(response1['message'] + ' ; ' + response2['message'])
    return response1
