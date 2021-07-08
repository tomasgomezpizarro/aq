import logging
import os

import sys
from logging.handlers import MemoryHandler

import atexit

from .async_logging_handler import AsyncStreamHandler


def init_logger():
    logger = logging.getLogger("market_connector")

    log_level = os.getenv("LOG_LEVEL", 'NULL')
    logger.setLevel(getattr(logging, log_level, getattr(logging, 'INFO')))

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    ch = AsyncStreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    return logger


def init_logger_2():
    logger = logging.getLogger("market_connector")

    logLevel = os.getenv("LOG_LEVEL", 'NULL')
    logger.setLevel(getattr(logging, logLevel, getattr(logging, 'INFO')))

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    streamhandler = logging.StreamHandler(sys.stdout)
    streamhandler.setLevel(getattr(logging, logLevel, getattr(logging, 'INFO')))
    streamhandler.setFormatter(formatter)

    memoryhandler = MemoryHandler(capacity=512, flushLevel=logging.ERROR, target=streamhandler)

    def flush():
        memoryhandler.flush()

    atexit.register(flush)

    logger.addHandler(memoryhandler)

    return logger

logger = init_logger()