import logging


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
    # set a format which is simpler for console use
    formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(message)s")
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)

    global logger
    logger = logging.getLogger(__name__)
