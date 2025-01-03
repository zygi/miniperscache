import logging


_logger = logging.getLogger("miniperscache")


def getLogger(child: str | None = None) -> logging.Logger:
    if child is None:
        return _logger
    else:
        return _logger.getChild(child)
