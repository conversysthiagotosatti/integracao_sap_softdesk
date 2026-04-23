from __future__ import annotations

from typing import Any

from django.test import TestCase, override_settings

from integration_bus.models import IntegrationEventReceipt
from softdesk_sync.client import SoftdeskAPIClient
from softdesk_sync.diff import payload_hash
from softdesk_sync.models import SoftdeskChamadoState, SoftdeskSyncState
from softdesk_sync.poller import run_poll


class _FakeClient(SoftdeskAPIClient):
    def __init__(self, pages: list[Any]) -> None:
        super().__init__(session=None, rate_limiter=None)
        self._pages = pages
        self.calls = 0

    def fetch_chamados_page(self, *, page=None, page_size=None, updated_since=None, extra_params=None):
        self.calls += 1
        idx = int(page or 1) - 1
        if 0 <= idx < len(self._pages):
            return self._pages[idx]
        return []


@override_settings(
    SOFTDESK_BASE_URL="https://softdesk.example",
    SOFTDESK_API_TOKEN="t",
    SOFTDESK_PAGE_SIZE=2,
    SOFTDESK_MAX_PAGES_PER_RUN=10,
    SOFTDESK_AUTO_INCREMENT_PAGE=True,
)
class PollerTests(TestCase):
    def test_detects_create_and_update(self) -> None:
        pages = [
            {"results": [{"id": "10", "updated_at": "2026-01-01T00:00:00Z", "title": "A"}]},
            {"results": []},
        ]
        client = _FakeClient(pages)
        with self.captureOnCommitCallbacks(execute=True):
            stats = run_poll(client=client)
        self.assertEqual(stats["created_events"], 1)
        self.assertEqual(
            SoftdeskChamadoState.objects.get(external_id="10").content_hash,
            payload_hash(pages[0]["results"][0]),
        )

        pages2 = [
            {"results": [{"id": "10", "updated_at": "2026-01-02T00:00:00Z", "title": "B"}]},
            {"results": []},
        ]
        client2 = _FakeClient(pages2)
        with self.captureOnCommitCallbacks(execute=True):
            stats2 = run_poll(client=client2)
        self.assertEqual(stats2["updated_events"], 1)
        self.assertEqual(IntegrationEventReceipt.objects.filter(event_type="softdesk.chamado.updated").count(), 1)

    def test_idempotent_rerun(self) -> None:
        pages = [
            {"results": [{"id": "11", "updated_at": "2026-01-01T00:00:00Z", "x": 1}]},
            {"results": []},
        ]
        client = _FakeClient(pages)
        with self.captureOnCommitCallbacks(execute=True):
            run_poll(client=client)
            run_poll(client=client)
        self.assertEqual(IntegrationEventReceipt.objects.count(), 1)

    def test_marks_sync_state_success(self) -> None:
        pages = [{"results": [{"id": "12", "updated_at": "2026-01-03T00:00:00Z"}]}, {"results": []}]
        with self.captureOnCommitCallbacks(execute=True):
            run_poll(client=_FakeClient(pages))
        st = SoftdeskSyncState.objects.get(scope="chamados")
        self.assertEqual(st.consecutive_failures, 0)
        self.assertTrue(st.last_poll_finished_at)
