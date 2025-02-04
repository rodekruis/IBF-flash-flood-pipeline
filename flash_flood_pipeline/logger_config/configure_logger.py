import logging
import sys
import traceback
import datetime
from settings.base import ENVIRONMENT


def configure_logger():
    """
    Configure logger (formatting and level) and set the logger to global to ensure it can be used throughout the pipeline.
    """
    print("configure_logger")
    logging.root.handlers = []
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s %(filename)s %(funcName)s %(lineno)d",  # "%(asctime)s : %(levelname)s : %(message)s",
        level=logging.INFO,
        filename="ex.log",
    )

    # set up logging to console
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.DEBUG)

    log_file = logging.FileHandler(
        rf"data/{ENVIRONMENT}/logs/container_log_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.txt"
    )
    
    log_file.setLevel(logging.INFO)

    # set a format which is simpler for console use
    formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(message)s")
    console.setFormatter(formatter)
    log_file.setFormatter(formatter)

    logging.getLogger("").addHandler(console)
    logging.getLogger("").addHandler(log_file)

    global logger
    logger = logging.getLogger(__name__)
    
    def log_exceptions(exctype, value, tb):
        logger.exception(''.join(traceback.format_exception(exctype, value, tb)))
        sys.__excepthook__(exctype, value, tb)

    sys.excepthook = log_exceptions


# logging.basicConfig( level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s %(filename)s %(funcName)s %(lineno)d', handlers=[ logging.FileHandler(‘app.log'), logging.StreamHandler() ] )
