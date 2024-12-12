import logging
import datetime


def configure_logger():
    """
    Configure logger (formatting and level) and set the logger to global to ensure it can be used throughout the pipeline.
    """
    logging.root.handlers = []
    logging.basicConfig(
        format="%(asctime)s : %(levelname)s : %(message)s",
        level=logging.INFO,
        filename="ex.log",
    )

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)

    log_file = logging.FileHandler(
        rf"data/logs/container_log_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log"
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
