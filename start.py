from celery import Celery
from utils.log import logger
from utils.env import env

app = Celery(
    "tasks",
    backend="redis://protector_redis:6379",
    broker="redis://protector_redis:6379/0",
    include=["fetch.stats"],
)

app.control.purge()

app.conf.beat_schedule = {
    "beats_scheduler": {
        "task": "fetch",
        "schedule": int(env.get("UPDATE_INTERVAL")),
    }
}

if __name__ == "__main__":
    logger.info("Starting celery tasks")
    app.start()
