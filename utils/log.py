import logging
import logging.handlers
import os
import sys

from utils.env import env

dirname = os.path.dirname(__file__)
LOGFILE = os.path.join(dirname, "../logs/protector.log")
LOG_LEVEL = logging.DEBUG if env.get("DEBUG") else logging.INFO

# Turn off urllib3 logger
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger("protector")
logger.setLevel(LOG_LEVEL)

handler = logging.handlers.RotatingFileHandler(
    LOGFILE, maxBytes=(1048576 * 5), backupCount=7
)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler(sys.stdout))
