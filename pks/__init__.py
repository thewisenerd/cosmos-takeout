import logging
import typing


def setup_logger(level: int | str = logging.INFO, handlers: typing.Optional[list[logging.Handler]] = None):
    if handlers is None:
        handlers = []

    logger = logging.getLogger('pks')
    logger.propagate = False
    logger.setLevel(level)
    for h in handlers:
        logger.addHandler(h)
