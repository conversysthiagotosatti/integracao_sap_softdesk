from django.core.management.base import BaseCommand

from integrations.crypto_helpers import encrypt_secret


class Command(BaseCommand):
    help = "Encrypt a SAP Service Layer password for storage in SapClientCredential.password_encrypted."

    def add_arguments(self, parser):
        parser.add_argument("password", type=str)

    def handle(self, *args, **options):
        token = encrypt_secret(options["password"])
        self.stdout.write(token)
