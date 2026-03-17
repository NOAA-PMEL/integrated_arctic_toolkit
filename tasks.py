from celery import Celery
import os

# Create the celery app pointing at redis
celery_app = Celery(
    "tasks",
    broker=os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"),
    backend=os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
)

# Schedule fetch_sst to run every 55 minutes automatically
celery_app.conf.beat_schedule = {
    "refresh-sst-cache": {
        "task": "tasks.refresh_sst",
        "schedule": 55 * 60, # seconds
    }
}

@celery_app.task
def refresh_sst():
    """Runs in background, keeps SST cache warm"""
    from data_queries import fetch_sst
    fetch_sst()
    print("SST cahce refreshed")

@celery_app.on_after_configure.connect
def run_on_startup(sender, **kwargs):
    refresh_sst.delay() # .delay means run this in the background. Tells celery to run refresh_sst once immediately when the worker starts