import logging
import logging.handlers
import os
import sys

dirname = os.path.dirname(__file__)
LOGFILE = os.path.join(dirname, "logs/fetcher.log")
LOG_LEVEL = logging.DEBUG if os.environ.get("DEBUG") else logging.INFO

# Turn off urllib3 logger
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger("")
logger.setLevel(LOG_LEVEL)

handler = logging.handlers.RotatingFileHandler(
    LOGFILE, maxBytes=(1048576 * 5), backupCount=7
)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler(sys.stdout))
