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


def merge_status(responses):
    if len(responses) == 0:
        return success_response()
    if len(responses) == 1:
        return responses[0]
    current_status = responses.pop(0)
    other_statuses = merge_status(responses)
    if is_successful(current_status):
        return other_statuses
    if is_fail(current_status) and is_fail(other_statuses):
        return failure_response(current_status['message'] + ' ; ' + other_statuses['message'])
    return current_status
