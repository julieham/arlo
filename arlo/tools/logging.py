import logging

from operations.date_operations import now
from operations.df_operations import set_pandas_print_parameters
from parameters.param import log_directory


def logging_filename():
    return log_directory + now().strftime('%Y_%m_%d_%a') + '.log'


def info(text):
    logging.basicConfig(filename=logging_filename(), level=logging.DEBUG)
    logging.info(text)


def info_df(df):
    set_pandas_print_parameters()
    logging.basicConfig(filename=logging_filename(), level=logging.DEBUG)
    logging.info('\n' + str(df))


def warn(text):
    logging.basicConfig(filename=logging_filename(), level=logging.DEBUG)
    logging.warning(text)


def debug(text):
    logging.basicConfig(filename=logging_filename(), level=logging.DEBUG)
    logging.debug(text)


def error(text):
    logging.basicConfig(filename=logging_filename(), level=logging.DEBUG)
    logging.error(text)


logging.basicConfig(filename=logging_filename(), level=logging.DEBUG)
