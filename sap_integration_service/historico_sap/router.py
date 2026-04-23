"""
Encaminha modelos deste app para o banco ``historico_clientes`` (alias ``conversys``).
"""
from __future__ import annotations

from typing import Any

from django.conf import settings


class HistoricoClientesRouter:
    """Usa ``DATABASES['conversys']`` (normalmente NAME = historico_clientes)."""

    route_app_labels = {"historico_sap"}

    def db_for_read(self, model: type, **hints: Any) -> str | None:
        if model._meta.app_label in self.route_app_labels:
            return "conversys" if "conversys" in settings.DATABASES else None
        return None

    def db_for_write(self, model: type, **hints: Any) -> str | None:
        return self.db_for_read(model, **hints)

    def allow_relation(self, obj1: Any, obj2: Any, **hints: Any) -> bool | None:
        dbs = {getattr(obj1._state, "db", None), getattr(obj2._state, "db", None)}
        if "conversys" in dbs and dbs != {"conversys"}:
            return False
        return None

    def allow_migrate(
        self,
        db: str,
        app_label: str,
        model_name: str | None = None,
        **hints: Any,
    ) -> bool | None:
        if app_label in self.route_app_labels:
            return db == "conversys"
        if db == "conversys":
            return False
        return None
