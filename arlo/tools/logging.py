import logging

from operations.date_operations import now
from parameters.param import log_directory


def logging_filename():
    return log_directory + now().strftime('%Y_%m_%d_%a') + '.log'


logging.basicConfig(filename=logging_filename(), level=logging.DEBUG)


def info(text):
    logging.info(text)


def warn(text):
    logging.warning(text)


def debug(text):
    logging.debug(text)


def error(text):
    logging.error(text)
