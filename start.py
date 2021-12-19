from celery import Celery
from utils.log import logger

app = Celery(
    "tasks",
    backend="redis://localhost:6379",
    broker="redis://localhost:6379/0",
    include=["fetch.stats"],
)

app.control.purge()

app.conf.beat_schedule = {
    "beats_scheduler": {
        "task": "fetch",
        "schedule": 60 * 5,  # Minutes
    }
}

if __name__ == "__main__":
    logger.info("Starting celery tasks")
    app.start()
