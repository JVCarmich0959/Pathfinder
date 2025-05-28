import logging
from typing import Optional


def setup_logging(name: str = __name__, log_file: Optional[str] = None,
                  level: int = logging.INFO) -> logging.Logger:
    """Set up and return a logger with a standard format.

    Parameters
    ----------
    name : str
        Name of the logger.
    log_file : Optional[str]
        Optional file path to also write log records.
    level : int
        Logging level, defaults to :data:`logging.INFO`.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """

    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(level=level, format=fmt, handlers=handlers, force=True)
    return logging.getLogger(name)
