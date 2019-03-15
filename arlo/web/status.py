success_value = 'SUCCESS'
fail_value = 'FAIL'

def my_response(success: object, message: object = '') -> object:
    return dict({'status': success_value if success else fail_value,
                 'message': message})


def success_response():
    return my_response(True)


def is_successful(response):
    return response['status'] == success_value


def is_fail(response):
    return response['status'] == fail_value
