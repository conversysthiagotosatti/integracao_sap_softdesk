from django.test import SimpleTestCase

from softdesk_sync.diff import payload_hash


class DiffTests(SimpleTestCase):
    def test_payload_hash_stable_key_order(self) -> None:
        a = {"z": 1, "a": {"nested": True}}
        b = {"a": {"nested": True}, "z": 1}
        self.assertEqual(payload_hash(a), payload_hash(b))
