from django.test import TestCase, override_settings

from integration_bus import bus as event_bus
from integration_bus.models import IntegrationEventReceipt
from integration_bus.signals import integration_event


@override_settings(
    SOFTDESK_BASE_URL="https://softdesk.example",
    SOFTDESK_API_TOKEN="t",
)
class IntegrationBusTests(TestCase):
    def setUp(self) -> None:
        self.received: list[dict] = []

        def handler(sender, **kwargs):
            self.received.append(kwargs)

        integration_event.connect(handler, weak=False)
        self.addCleanup(integration_event.disconnect, handler)

    def test_publish_dedupes_identical_payload(self) -> None:
        payload = {"id": "1", "updated_at": "2020-01-01T00:00:00Z"}
        h = event_bus.make_dedup_key("softdesk.chamado.created", "1", "abc")
        self.assertEqual(len(h), 64)

        with self.captureOnCommitCallbacks(execute=True):
            first = event_bus.publish(
                integration="softdesk",
                event_type="softdesk.chamado.created",
                external_id="1",
                payload=payload,
                content_hash="abc",
            )
            second = event_bus.publish(
                integration="softdesk",
                event_type="softdesk.chamado.created",
                external_id="1",
                payload=payload,
                content_hash="abc",
            )
        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(IntegrationEventReceipt.objects.count(), 1)
        self.assertEqual(len(self.received), 1)

    def test_publish_allows_same_ticket_different_hash(self) -> None:
        with self.captureOnCommitCallbacks(execute=True):
            event_bus.publish(
                integration="softdesk",
                event_type="softdesk.chamado.updated",
                external_id="1",
                payload={"id": "1", "v": 1},
                content_hash="h1",
            )
            event_bus.publish(
                integration="softdesk",
                event_type="softdesk.chamado.updated",
                external_id="1",
                payload={"id": "1", "v": 2},
                content_hash="h2",
            )
        self.assertEqual(IntegrationEventReceipt.objects.count(), 2)
        self.assertEqual(len(self.received), 2)
