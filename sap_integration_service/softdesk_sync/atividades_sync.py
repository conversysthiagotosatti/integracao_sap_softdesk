"""
Sincroniza o bloco ``atividades`` do JSON do dossiê (RetornaDossie) para ``helpdesk_atividadechamado``.

A gravação é feita **somente com SQL** no PostgreSQL configurado em ``DATABASES['conversys']``
(sem chamadas HTTP à API do Conversys para persistir linhas).

Mapeamento automático (de/para) com aliases configuráveis via ``DOSSIE_ATIVIDADE_FIELD_MAP_JSON``.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from django.conf import settings
from django.utils import dateparse
from django.utils import timezone as dj_tz

logger = logging.getLogger(__name__)

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

_TIPOS_VALIDOS = frozenset({"INTERNA", "PUBLICA", "REMOTA", "PRESENCIAL", "TELEFONE"})

_DEFAULT_ALIASES: dict[str, list[str]] = {
    "descricao": [
        "descricao",
        "texto",
        "observacao",
        "conteudo",
        "historico",
        "detalhe",
        "comentario",
        "mensagem",
        "descricaoAtividade",
    ],
    "tipo_atividade": ["tipo_atividade", "tipo", "tipoAtividade", "tipo_atividade_chamado"],
    "tempo_gasto_minutos": [
        "tempo_gasto_minutos",
        "tempoMinutos",
        "duracao_minutos",
        "minutos",
        "tempo_minutos",
        "duracaoMinutos",
    ],
    "tempo_horas": ["tempo_horas", "horas", "duracao_horas", "tempoHoras"],
    "publico": ["publico", "visivel", "is_public", "publica", "visivelCliente"],
    "data_inicio": ["data_inicio", "dataInicio", "inicio", "dt_inicio", "data_inicial", "criado_em"],
    "data_fim": ["data_fim", "dataFim", "fim", "dt_fim", "data_final", "atualizado_em"],
    "codigo_integracao": [
        "codigo_integracao",
        "codigoIntegracao",
        "id",
        "codigo",
        "id_atividade",
        "numero",
        "nr_atividade",
    ],
    "atendente_id": ["atendente_id", "atendenteId", "codigo_atendente", "id_atendente"],
    "usuario_id": ["usuario_id", "usuarioId", "user_id", "id_usuario"],
}


def _merged_aliases() -> dict[str, list[str]]:
    merged = {k: list(v) for k, v in _DEFAULT_ALIASES.items()}
    raw = getattr(settings, "DOSSIE_ATIVIDADE_FIELD_MAP_JSON", "") or ""
    if not raw.strip():
        return merged
    try:
        extra = json.loads(raw)
        if isinstance(extra, dict):
            for col, aliases in extra.items():
                if not isinstance(col, str) or not isinstance(aliases, list):
                    continue
                key = col.strip()
                if key not in merged:
                    merged[key] = []
                for a in aliases:
                    if isinstance(a, str) and a.strip() and a.strip() not in merged[key]:
                        merged[key].append(a.strip())
    except json.JSONDecodeError:
        logger.warning("DOSSIE_ATIVIDADE_FIELD_MAP_JSON inválido.")
    return merged


def _pick(item: dict[str, Any], aliases: list[str]) -> Any:
    if not isinstance(item, dict):
        return None
    keys_lower = {str(k).lower(): k for k in item}
    for a in aliases:
        al = a.lower()
        if al in keys_lower:
            return item[keys_lower[al]]
        if a in item:
            return item[a]
    return None


def _parse_ts(val: Any) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if dj_tz.is_aware(val) else dj_tz.make_aware(val, dj_tz.get_current_timezone())
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(float(val), tz=dj_tz.utc)
        except (OSError, ValueError, OverflowError):
            return None
    s = str(val).strip()
    if not s:
        return None
    dt = dateparse.parse_datetime(s)
    if dt:
        return dt if dj_tz.is_aware(dt) else dj_tz.make_aware(dt, dj_tz.utc)
    try:
        d = dateparse.parse_date(s)
        if d:
            return dj_tz.make_aware(datetime.combine(d, datetime.min.time()), dj_tz.utc)
    except Exception:  # noqa: BLE001
        pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_bool(val: Any, default: bool = True) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("0", "false", "não", "nao", "no", "off"):
        return False
    if s in ("1", "true", "sim", "yes", "on"):
        return True
    return default


def _norm_tipo_atividade(raw: Any) -> str:
    if raw is None or str(raw).strip() == "":
        return "PUBLICA"
    s = str(raw).strip()
    up = s.upper()
    if up in _TIPOS_VALIDOS:
        return up
    low = s.lower()
    pt = {
        "interna": "INTERNA",
        "pública": "PUBLICA",
        "publica": "PUBLICA",
        "remota": "REMOTA",
        "presencial": "PRESENCIAL",
        "telefone": "TELEFONE",
    }
    return pt.get(low, "PUBLICA")


def _parse_int(val: Any) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(Decimal(str(val)))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _dict_candidates_for_atividades(inner: dict[str, Any], dossie: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Dicionários onde a lista pode aparecer: ``inner``, raiz do dossiê e dicts aninhados
    (ex.: ``{ retorno: { chamado: {...}, atividades: [] } }`` — ``atividades`` fica fora de ``inner``).
    """
    seen_ids: set[int] = set()
    out: list[dict[str, Any]] = []

    def add(d: dict[str, Any]) -> None:
        if not isinstance(d, dict):
            return
        d2 = _strip_meta_keys(d)
        i = id(d2)
        if i in seen_ids:
            return
        seen_ids.add(i)
        out.append(d2)

    if isinstance(inner, dict):
        add(inner)
    if isinstance(dossie, dict):
        add(_strip_meta_keys(dossie))
        for v in dossie.values():
            if isinstance(v, dict):
                add(_strip_meta_keys(v))
                for v2 in v.values():
                    if isinstance(v2, dict):
                        add(_strip_meta_keys(v2))
                        for v3 in v2.values():
                            if isinstance(v3, dict):
                                add(_strip_meta_keys(v3))
    return out


def extract_atividades_list(
    inner: dict[str, Any], dossie: dict[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Localiza lista de atividades no objeto do chamado ou na raiz / ramos do dossiê.

    Retorna ``(itens, meta_diagnostico)`` — ``meta`` ajuda quando nada é encontrado.
    """
    candidates = _dict_candidates_for_atividades(inner, dossie)

    list_keys = getattr(settings, "DOSSIE_ATIVIDADE_LIST_KEYS", "")
    if list_keys.strip():
        try:
            extra_keys = json.loads(list_keys)
            if isinstance(extra_keys, list):
                keys_try = [str(x) for x in extra_keys if str(x).strip()]
            else:
                keys_try = []
        except json.JSONDecodeError:
            keys_try = []
    else:
        keys_try = [
            "atividades",
            "Atividades",
            "listaAtividades",
            "ListaAtividades",
            "historicoAtividades",
            "HistoricoAtividades",
            "atividadeChamado",
            "atividade_list",
            "AtividadeList",
            "historico_atividades",
        ]

    sample_keys: list[str] = []
    for src in candidates[:10]:
        sample_keys.extend([k for k in src if isinstance(k, str)][:40])
    debug: dict[str, Any] = {
        "keys_try": keys_try,
        "candidate_dicts": len(candidates),
        "chaves_em_dicts_candidatos": sorted(set(sample_keys))[:100],
    }

    def try_src(src: dict[str, Any], key: str) -> tuple[list[dict[str, Any]], str] | None:
        v = src.get(key)
        if isinstance(v, list):
            dict_items = [x for x in v if isinstance(x, dict)]
            if dict_items:
                return dict_items, key
        if isinstance(v, dict):
            for subk in ("items", "data", "lista", "rows", "registros"):
                inner_list = v.get(subk)
                if isinstance(inner_list, list):
                    dict_items = [x for x in inner_list if isinstance(x, dict)]
                    if dict_items:
                        return dict_items, f"{key}.{subk}"
        return None

    for src in candidates:
        key_by_lower = {str(k).lower(): k for k in src if isinstance(k, str)}
        for key in keys_try:
            hit = try_src(src, key)
            if hit:
                items, mk = hit
                via = "lista_aninhada" if "." in mk else "lista_direta"
                return items, {**debug, "matched_key": mk, "via": via}
            lk = key.lower()
            if lk in key_by_lower and key_by_lower[lk] != key:
                hit = try_src(src, str(key_by_lower[lk]))
                if hit:
                    items, mk = hit
                    via = "lista_aninhada" if "." in mk else "chave_case_insensitive"
                    return items, {**debug, "matched_key": mk, "via": via}

    return [], {**debug, "matched_key": None}


def _strip_meta_keys(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if isinstance(k, str) and not k.startswith("_")}


def _fk_exists(cursor, qn: Any, table: str, pk: int | None) -> bool:
    if pk is None or pk <= 0:
        return False
    if not _TABLE_RE.match(table):
        return False
    cursor.execute(f"SELECT 1 FROM {qn(table)} WHERE {qn('id')} = %s LIMIT 1", [pk])
    return cursor.fetchone() is not None


def map_atividade_row(item: dict[str, Any], *, chamado_id: int, aliases: dict[str, list[str]]) -> dict[str, Any]:
    """Retorna colunas prontas para INSERT/UPDATE (sem id)."""
    desc = _pick(item, aliases["descricao"])
    descricao = (str(desc).strip() if desc is not None else "") or "(atividade sem descrição)"

    tipo_raw = _pick(item, aliases["tipo_atividade"])
    tipo = _norm_tipo_atividade(tipo_raw)[:20]

    mins = _parse_int(_pick(item, aliases["tempo_gasto_minutos"]))
    if mins is None:
        horas = _pick(item, aliases.get("tempo_horas", []))
        if horas is not None:
            try:
                mins = int(float(str(horas).replace(",", ".")) * 60)
            except ValueError:
                mins = 0
    if mins is None:
        mins = 0
    if mins < 0:
        mins = 0

    publico = _parse_bool(_pick(item, aliases["publico"]), default=True)

    di = _parse_ts(_pick(item, aliases["data_inicio"]))
    df = _parse_ts(_pick(item, aliases["data_fim"]))

    cod_int = _parse_int(_pick(item, aliases["codigo_integracao"]))
    if cod_int is not None and cod_int <= 0:
        cod_int = None

    atendente_id = _parse_int(_pick(item, aliases["atendente_id"]))
    usuario_id = _parse_int(_pick(item, aliases["usuario_id"]))

    return {
        "chamado_id": chamado_id,
        "descricao": descricao,
        "tipo_atividade": tipo,
        "tempo_gasto_minutos": mins,
        "publico": publico,
        "data_inicio": di,
        "data_fim": df,
        "codigo_integracao": cod_int,
        "atendente_id": atendente_id,
        "usuario_id": usuario_id,
    }


def persist_atividades_from_dossie(
    *,
    cursor: Any,
    qn: Any,
    chamado_id: int,
    inner: dict[str, Any],
    dossie: dict[str, Any],
    steps: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Grava atividades na tabela ``helpdesk_atividadechamado`` (upsert por ``codigo_integracao``
    quando informado; caso contrário apenas INSERT).
    """
    tbl = (getattr(settings, "CONVERSYS_ATIVIDADE_TABLE", "helpdesk_atividadechamado") or "").strip()
    if not _TABLE_RE.match(tbl):
        return {"ok": False, "detail": "CONVERSYS_ATIVIDADE_TABLE inválido."}

    atendente_tbl = (getattr(settings, "CONVERSYS_ATENDENTE_TABLE", "helpdesk_atendente") or "").strip()
    user_tbl = (getattr(settings, "CONVERSYS_USER_TABLE", "auth_user") or "").strip()

    items, extract_meta = extract_atividades_list(inner, dossie)
    if not items:
        steps.append(
            {
                "phase": "11_atividades",
                "message": "Nenhuma lista de atividades encontrada no JSON",
                "data": extract_meta,
            }
        )
        return {
            "ok": True,
            "processed": 0,
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
            "errors": [],
            "campos_mapeados": list(_merged_aliases().keys()),
            "extracao_json": extract_meta,
        }

    aliases = _merged_aliases()
    steps.append(
        {
            "phase": "11_atividades",
            "message": f"Encontradas {len(items)} atividade(s) no JSON",
            "data": {
                "tabela": tbl,
                "persistencia": "sql_postgres_conversys",
                "extracao": {k: extract_meta.get(k) for k in ("matched_key", "via", "candidate_dicts")},
                "aliases": {k: v[:5] for k, v in aliases.items()},
            },
        }
    )

    inserted = 0
    updated = 0
    skipped = 0
    errors: list[dict[str, Any]] = []
    now = dj_tz.now()

    for idx, raw in enumerate(items):
        try:
            row = map_atividade_row(raw, chamado_id=chamado_id, aliases=aliases)

            aid = row["atendente_id"]
            if aid is not None and not _fk_exists(cursor, qn, atendente_tbl, aid):
                row["atendente_id"] = None
            uid = row["usuario_id"]
            if uid is not None and not _fk_exists(cursor, qn, user_tbl, uid):
                row["usuario_id"] = None

            cod = row["codigo_integracao"]
            if cod is not None:
                cursor.execute(
                    f"""
                    UPDATE {qn(tbl)} SET
                        {qn("descricao")} = %s,
                        {qn("tipo_atividade")} = %s,
                        {qn("tempo_gasto_minutos")} = %s,
                        {qn("publico")} = %s,
                        {qn("data_inicio")} = %s,
                        {qn("data_fim")} = %s,
                        {qn("atendente_id")} = %s,
                        {qn("usuario_id")} = %s,
                        {qn("atualizado_em")} = %s
                    WHERE {qn("chamado_id")} = %s AND {qn("codigo_integracao")} = %s
                    """,
                    [
                        row["descricao"],
                        row["tipo_atividade"],
                        row["tempo_gasto_minutos"],
                        row["publico"],
                        row["data_inicio"],
                        row["data_fim"],
                        row["atendente_id"],
                        row["usuario_id"],
                        now,
                        chamado_id,
                        cod,
                    ],
                )
                if cursor.rowcount > 0:
                    updated += 1
                    continue

            cursor.execute(
                f"""
                INSERT INTO {qn(tbl)} (
                    {qn("chamado_id")},
                    {qn("descricao")},
                    {qn("tipo_atividade")},
                    {qn("tempo_gasto_minutos")},
                    {qn("publico")},
                    {qn("data_inicio")},
                    {qn("data_fim")},
                    {qn("codigo_integracao")},
                    {qn("atendente_id")},
                    {qn("usuario_id")},
                    {qn("criado_em")},
                    {qn("atualizado_em")}
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    row["chamado_id"],
                    row["descricao"],
                    row["tipo_atividade"],
                    row["tempo_gasto_minutos"],
                    row["publico"],
                    row["data_inicio"],
                    row["data_fim"],
                    row["codigo_integracao"],
                    row["atendente_id"],
                    row["usuario_id"],
                    now,
                    now,
                ],
            )
            inserted += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("atividade_sync.row_failed", extra={"index": idx})
            errors.append({"index": idx, "erro": str(exc)[:500]})
            skipped += 1

    steps.append(
        {
            "phase": "12_atividades_resultado",
            "message": "Persistência de atividades concluída",
            "data": {
                "inseridas": inserted,
                "atualizadas": updated,
                "ignoradas_erro": skipped,
                "erros": errors[:10],
            },
        }
    )

    return {
        "ok": len(errors) == 0,
        "processed": len(items),
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "tabela": tbl,
        "campos_mapeados": list(aliases.keys()),
        "aliases_por_campo": {k: v[:8] for k, v in aliases.items()},
        "extracao_json": extract_meta,
    }
