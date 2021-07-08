import logging


def init_logging(name_logger):
    """
    Configuro logging para llevar registro del AQhandler.
    """
    aq_logger = logging.getLogger(name_logger)
    aq_logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s')

    fh = logging.FileHandler('AQHandler.log')
    fh.setFormatter(formatter)
    aq_logger.addHandler(fh)

    return aq_logger


logger = init_logging('aq_handler')
