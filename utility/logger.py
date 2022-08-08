import sys
import logging


def setup_logger_cli(filename_stem):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_formatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
    file_handler = logging.FileHandler(f'{filename_stem}.log', 'w')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)

    return logger


def setup_logger_lambda():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_formatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)

    return logger
