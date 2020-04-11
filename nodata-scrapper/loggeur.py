import logging
import os
import sys


def create_logger():

    # create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to warning
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)

    # create file handler and set level to warning
    file_handler = logging.FileHandler(os.path.join(sys.path[0], "log_file.txt"))
    file_handler.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to handlers
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # add handlers to logger
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger

