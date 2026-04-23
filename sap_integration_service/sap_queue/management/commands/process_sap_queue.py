from django.core.management.base import BaseCommand

from sap_queue.queue_service import process_queue


class Command(BaseCommand):
    help = "Process pending SAP queue rows (sync worker; Celery can call the same function)."

    def add_arguments(self, parser):
        parser.add_argument("--batch-size", type=int, default=10)
        parser.add_argument(
            "--company-id",
            type=str,
            default="",
            help="Processar apenas itens deste company_id (opcional).",
        )

    def handle(self, *args, **options):
        cid = (options.get("company_id") or "").strip() or None
        n = process_queue(batch_size=options["batch_size"], company_id=cid)
        self.stdout.write(self.style.SUCCESS(f"Processed {n} queue item(s)."))
