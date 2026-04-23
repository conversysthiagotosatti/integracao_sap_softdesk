from core.celery import app
from sap_queue.queue_service import process_queue


@app.task(name="sap.process_queue")
def process_sap_queue(batch_size: int = 10) -> int:
    """Celery entrypoint: drain database queue."""
    return process_queue(batch_size=batch_size)
