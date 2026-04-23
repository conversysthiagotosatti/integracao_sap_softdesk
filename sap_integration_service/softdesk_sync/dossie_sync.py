"""
Após ``RetornaDossie``, relaciona o JSON ao chamado no PostgreSQL do Conversys
(``historico_clientes``) e aplica UPDATE + linhas em ``helpdesk_chamadohistorico``.

O campo ``status`` do JSON é resolvido para o **id** da tabela de configuração de status
(``CONVERSYS_STATUS_CHAMADO_CONFIG_TABLE``), casando com ``codigo_integracao``, e esse id
é gravado na coluna física do chamado (padrão ``status_id`` — ver ``SOFTDESK_CHAMADO_STATUS_FIELD``).
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from django.conf import settings
from django.db import connections, transaction
from django.utils import timezone

from softdesk_sync.atividades_sync import persist_atividades_from_dossie

logger = logging.getLogger(__name__)

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _as_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, dict):
        for sub in ("nome", "descricao", "label", "titulo", "codigo", "id"):
            if sub in v and v[sub] is not None:
                return _as_text(v[sub])
        return ""
    return str(v).strip()


def _strip_meta(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if isinstance(k, str) and not k.startswith("_")}


def _ci_key_map(d: dict[str, Any]) -> dict[str, str]:
    return {str(k).lower(): k for k in d if isinstance(k, str) and not str(k).startswith("_")}


def _ci_get(d: dict[str, Any], key: str) -> Any:
    m = _ci_key_map(d)
    ak = m.get(key.lower())
    return d.get(ak) if ak is not None else None


def _dict_has_ticket_signals(x: dict[str, Any]) -> bool:
    """Campos comuns do chamado no Soft4 / Conversys (inclui camelCase por chave normalizada)."""
    if not isinstance(x, dict) or not x:
        return False
    key_lower_to_orig = _ci_key_map(x)
    lower_keys = set(key_lower_to_orig.keys())
    wanted = {
        "titulo",
        "assunto",
        "status",
        "id",
        "codigo",
        "numero",
        "nr_chamado",
        "nrchamado",
        "descricao",
        "detalhes",
        "mensagem",
        "observacao",
        "codigo_helpdesk_api",
        "codigohelpdeskapi",
        "softdesk_id",
        "softdeskid",
        "prioridade",
        "categoria",
        "cliente",
        "solicitante",
    }
    if lower_keys & wanted:
        return True
    for act in ("atividades", "listaatividades", "historicoatividades"):
        orig = key_lower_to_orig.get(act)
        if orig and isinstance(x.get(orig), list):
            return True
    return False


def _default_chamado_branch_keys() -> list[str]:
    return [
        "chamado",
        "data",
        "dados",
        "objeto",
        "ticket",
        "registro",
        "item",
        "retorno",
        "resultado",
        "response",
        "payload",
        "content",
        "result",
        "consulta",
        "dossie",
        "informacoes",
        "informações",
        "dadosChamado",
        "dados_chamado",
        "dadoschamado",
        "chamadoSoftdesk",
        "chamadosoftdesk",
    ]


def _chamado_branch_keys_from_settings() -> list[str]:
    raw = getattr(settings, "DOSSIE_CHAMADO_BRANCH_KEYS", "") or ""
    if not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if str(x).strip()]
    except json.JSONDecodeError:
        logger.warning("DOSSIE_CHAMADO_BRANCH_KEYS inválido; ignorando.")
    return []


def _deep_find_ticket_dict(
    root: dict[str, Any], *, max_depth: int = 7, max_nodes: int = 400
) -> tuple[dict[str, Any], str]:
    """BFS: primeiro dict que pareça chamado (ou contenha lista de atividades)."""
    from collections import deque

    seen: set[int] = set()
    q: deque[tuple[dict[str, Any], str, int]] = deque()
    q.append((root, "", 0))

    while q and len(seen) < max_nodes:
        node, path, depth = q.popleft()
        i = id(node)
        if i in seen:
            continue
        seen.add(i)
        if _dict_has_ticket_signals(node):
            label = path if path else "(profundidade)"
            return _strip_meta(node), label
        if depth >= max_depth:
            continue

        for k, v in node.items():
            if not isinstance(k, str) or k.startswith("_"):
                continue
            child_path = f"{path}.{k}" if path else k
            if isinstance(v, dict):
                q.append((v, child_path, depth + 1))
            elif isinstance(v, list):
                for idx, el in enumerate(v[:50]):
                    if isinstance(el, dict):
                        q.append((el, f"{child_path}[{idx}]", depth + 1))
    return {}, ""


def _primary_ticket_dict(d: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """
    Retorna (dict interno, nome do ramo usado).

    Aceita wrappers (``retorno``, ``resultado``, …), chaves em qualquer capitalização,
    listas ``chamados: [ {{...}} ]`` e busca em profundidade quando o layout não é plano.
    """
    d0 = _strip_meta(d)
    if not d0:
        return {}, ""

    branch_try = _chamado_branch_keys_from_settings() + _default_chamado_branch_keys()
    seen_branches: set[str] = set()
    ordered_branches: list[str] = []
    for bk in branch_try:
        low = bk.lower()
        if low not in seen_branches:
            seen_branches.add(low)
            ordered_branches.append(bk)

    for key in ordered_branches:
        v = _ci_get(d0, key)
        if isinstance(v, dict):
            inner = _strip_meta(v)
            if _dict_has_ticket_signals(inner):
                actual = _ci_key_map(d0).get(key.lower(), key)
                return inner, actual
        if isinstance(v, list):
            for idx, el in enumerate(v):
                if isinstance(el, dict):
                    inner = _strip_meta(el)
                    if _dict_has_ticket_signals(inner):
                        actual = _ci_key_map(d0).get(key.lower(), key)
                        return inner, f"{actual}[{idx}]"

    if _dict_has_ticket_signals(d0):
        return d0, "(raiz)"

    inner, path = _deep_find_ticket_dict(d0)
    if inner:
        return inner, path

    return {}, ""


def _pick_titulo(inner: dict[str, Any]) -> str | None:
    for k in ("titulo", "assunto", "subject", "nome"):
        s = _as_text(inner.get(k))
        if s:
            return s[:255]
    return None


def _pick_descricao(inner: dict[str, Any]) -> str | None:
    for k in ("descricao", "detalhes", "mensagem", "observacao", "texto"):
        s = _as_text(inner.get(k))
        if s:
            return s
    return None


def _needles_from_status_payload(val: Any) -> list[str]:
    """
    Valores a casar com ``codigo_integracao`` (ordem: códigos numéricos primeiro, depois textos).

    Aceita objeto Soft4 típico: ``{"codigo": 1, "descricao": "Em atendimento"}``.
    """
    seen: set[str] = set()
    ordered: list[str] = []

    def add(s: str) -> None:
        t = (s or "").strip()
        if not t or t in seen:
            return
        seen.add(t)
        ordered.append(t)

    if isinstance(val, dict):
        dm = {str(k).lower(): k for k in val if isinstance(k, str)}
        for lk in (
            "codigo",
            "id",
            "codigointegracao",
            "codigo_integracao",
            "numerostatus",
            "numero",
        ):
            orig = dm.get(lk)
            if orig is None:
                continue
            v = val.get(orig)
            if v is None or isinstance(v, (dict, list)):
                continue
            add(str(v).strip())
        for lk2 in ("descricao", "nome", "label", "titulo", "denominacao", "descricao_status"):
            orig = dm.get(lk2)
            if orig is None:
                continue
            s = _as_text(val.get(orig))
            if s:
                add(s)
    else:
        s = _as_text(val)
        if s:
            add(s)
    return ordered


def _status_integration_needles(inner: dict[str, Any]) -> list[str]:
    """Lista ordenada de candidatos a ``codigo_integracao`` a partir do bloco de status do JSON."""
    ordered: list[str] = []
    seen: set[str] = set()
    m = _ci_key_map(inner)
    for alias in (
        "status",
        "statuschamado",
        "status_chamado",
        "situacao",
        "estado",
        "state",
        "codigostatus",
        "codigo_status",
    ):
        orig = m.get(alias)
        if orig is None:
            continue
        val = inner.get(orig)
        for n in _needles_from_status_payload(val):
            if n not in seen:
                seen.add(n)
                ordered.append(n)
    return ordered


def _coerce_int_id(v: Any) -> int | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _lookup_status_chamado_config_id(
    cursor: Any, qn: Any, cfg_table: str, status_raw: str
) -> tuple[int | None, str]:
    """
    ``SELECT id`` na tabela de status (ex.: ``helpdesk_statuschamadoconfig``)
    onde ``codigo_integracao`` = texto vindo do JSON (trim; segunda tentativa case-insensitive).
    """
    needle = (status_raw or "").strip()
    if not needle:
        return None, "status_json_vazio"
    if not _TABLE_RE.match(cfg_table):
        return None, "tabela_config_invalida"

    col_ci = qn("codigo_integracao")
    col_id = qn("id")
    tbl = qn(cfg_table)

    for use_lower in (False, True):
        if not use_lower:
            where = f"TRIM(CAST({col_ci} AS TEXT)) = TRIM(%s)"
        else:
            where = f"LOWER(TRIM(CAST({col_ci} AS TEXT))) = LOWER(TRIM(%s))"
        cursor.execute(
            f"SELECT {col_id} FROM {tbl} WHERE {where} LIMIT 2",
            [needle],
        )
        rows = cursor.fetchall()
        if len(rows) == 1:
            rid = _coerce_int_id(rows[0][0])
            return rid, "ok_ci" if use_lower else "ok"
        if len(rows) > 1:
            rid = _coerce_int_id(rows[0][0])
            logger.warning(
                "dossie_sync.status_config_duplicado",
                extra={"cfg_table": cfg_table, "needle": needle, "qtd": len(rows)},
            )
            return rid, "multiplo_uso_primeiro"
    return None, "sem_match_codigo_integracao"


def _lookup_status_chamado_config_first_match(
    cursor: Any, qn: Any, cfg_table: str, needles: list[str]
) -> tuple[int | None, str, str | None]:
    """Tenta cada needle até encontrar um ``id`` na tabela de config."""
    if not needles:
        return None, "status_json_vazio", None
    if not _TABLE_RE.match(cfg_table):
        return None, "tabela_config_invalida", None
    last_detail = "sem_match_codigo_integracao"
    for needle in needles:
        rid, detail = _lookup_status_chamado_config_id(cursor, qn, cfg_table, needle)
        last_detail = detail
        if rid is not None:
            return rid, f"{detail} (codigo_integracao≈{needle!r})", needle
    return None, f"{last_detail} tentativas={needles!r}", None


def _pick_softdesk_id(inner: dict[str, Any]) -> tuple[int | None, str]:
    for k in ("id", "codigo", "numero", "nr_chamado"):
        v = inner.get(k)
        if v is None:
            continue
        try:
            return int(str(v).strip()), f"{k}={v!r}"
        except ValueError:
            continue
    return None, ""


def _dossie_is_error_response(dossie: dict[str, Any]) -> bool:
    if dossie.get("_error") or dossie.get("_raw_body") or dossie.get("_empty_body"):
        return True
    st = dossie.get("_http_status")
    if isinstance(st, int) and st >= 400:
        return True
    return False


def _base_result(
    *,
    ok: bool,
    steps: list[dict[str, Any]],
    detail: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": ok, "steps": steps, "finished_at": timezone.now().isoformat()}
    if detail:
        out["detail"] = detail
    out.update(extra)
    return out


def sync_chamado_from_dossie(codigo_helpdesk_api: str, dossie: dict[str, Any]) -> dict[str, Any]:
    """
    Localiza ``helpdesk_chamado`` por ``codigo_helpdesk_api``, aplica mudanças vindas do dossiê
    e grava ``helpdesk_chamadohistorico``. O status do dossiê vira FK via tabela de config.

    Retorno inclui ``steps`` (fases), ``dossie_extraction``, ``field_comparisons``, ``database``.
    """
    steps: list[dict[str, Any]] = []

    def step(phase: str, message: str, **kw: Any) -> None:
        entry: dict[str, Any] = {"phase": phase, "message": message}
        if kw:
            entry["data"] = kw
        steps.append(entry)

    codigo = (codigo_helpdesk_api or "").strip()
    step("1_entrada", "Início da sincronização", codigo_helpdesk_api=codigo)

    if not codigo:
        return _base_result(ok=False, steps=steps, detail="codigo_helpdesk_api vazio.")

    if "conversys" not in settings.DATABASES:
        return _base_result(
            ok=False,
            steps=steps,
            detail="Banco Conversys não configurado (CONVERSYS_POSTGRES_DB).",
        )

    meta_keys = [k for k in dossie if isinstance(k, str) and k.startswith("_")]
    step("2_resposta_dossie", "Metadados da resposta HTTP", keys=meta_keys)

    if _dossie_is_error_response(dossie):
        return _base_result(
            ok=False,
            steps=steps,
            detail="Resposta do dossiê indica erro ou corpo inválido; nada foi gravado.",
            dossie_error_keys=[k for k in ("_error", "_raw_body", "_empty_body", "_http_status") if k in dossie],
        )

    chamado_tbl = (getattr(settings, "CONVERSYS_CHAMADOS_TABLE", "helpdesk_chamado") or "").strip()
    hist_tbl = (getattr(settings, "CONVERSYS_CHAMADO_HISTORICO_TABLE", "helpdesk_chamadohistorico") or "").strip()
    status_col = (getattr(settings, "SOFTDESK_CHAMADO_STATUS_FIELD", "status_id") or "status_id").strip()
    cfg_tbl = (
        getattr(settings, "CONVERSYS_STATUS_CHAMADO_CONFIG_TABLE", "helpdesk_statuschamadoconfig") or ""
    ).strip()
    if not _TABLE_RE.match(chamado_tbl) or not _TABLE_RE.match(hist_tbl):
        return _base_result(ok=False, steps=steps, detail="Nome de tabela inválido.")
    if not _TABLE_RE.match(status_col):
        return _base_result(
            ok=False,
            steps=steps,
            detail="SOFTDESK_CHAMADO_STATUS_FIELD inválido (apenas letras, números e _).",
        )
    if cfg_tbl and not _TABLE_RE.match(cfg_tbl):
        return _base_result(
            ok=False,
            steps=steps,
            detail="CONVERSYS_STATUS_CHAMADO_CONFIG_TABLE inválido (apenas letras, números e _).",
        )

    step(
        "3_tabelas",
        "Tabelas alvo",
        chamado=chamado_tbl,
        historico=hist_tbl,
        chamado_status_col=status_col,
        status_config=cfg_tbl or "(desligado — sem resolução de status por codigo_integracao)",
    )

    inner, branch = _primary_ticket_dict(dossie)
    root_keys = list(_strip_meta(dossie).keys())[:40]
    step("4_parse_json", "Extração do objeto de chamado no JSON", branch=branch or "(nenhum)", root_keys=root_keys)

    if not inner:
        step(
            "4b_parse_json_ajuda",
            "Nenhum dict com campos típicos de chamado; confira a estrutura ou DOSSIE_CHAMADO_BRANCH_KEYS",
            root_keys_amostra=root_keys,
            dica_env="DOSSIE_CHAMADO_BRANCH_KEYS (JSON array de chaves na raiz, ex.: [\"retorno\",\"dados\"])",
        )
        return _base_result(
            ok=False,
            steps=steps,
            detail=(
                "JSON do dossiê sem objeto de chamado reconhecível. "
                "Se o chamado estiver aninhado, defina DOSSIE_CHAMADO_BRANCH_KEYS com o nome da chave "
                "que contém o objeto (ex.: [\"retorno\"])."
            ),
        )

    novo_titulo = _pick_titulo(inner)
    nova_desc = _pick_descricao(inner)
    status_needles = _status_integration_needles(inner)
    status_raw = status_needles[0] if status_needles else ""
    novo_sid, sid_source = _pick_softdesk_id(inner)

    extraction: dict[str, Any] = {
        "branch": branch,
        "titulo": novo_titulo,
        "descricao_len": len(nova_desc) if nova_desc else 0,
        "descricao_preview": (nova_desc[:200] + "…") if nova_desc and len(nova_desc) > 200 else nova_desc,
        "status_raw": status_raw or None,
        "status_needles": status_needles,
        "status_config_id": None,
        "status_lookup_detail": None,
        "status_config_table": cfg_tbl or None,
        "softdesk_id": novo_sid,
        "softdesk_id_source": sid_source or None,
    }
    step(
        "5_campos_extraidos",
        "Valores interpretados para o chamado (status: string ou objeto codigo/descricao)",
        **extraction,
    )

    comparisons: list[dict[str, Any]] = []

    conn = connections["conversys"]
    qn = conn.ops.quote_name

    try:
        with transaction.atomic(using="conversys"):
            with conn.cursor() as cursor:
                step(
                    "6_sql_select",
                    "SELECT no PostgreSQL (Conversys)",
                    sql=f"FROM {chamado_tbl} WHERE codigo_helpdesk_api = %s",
                )
                cursor.execute(
                    f"""
                    SELECT {qn("id")}, {qn("titulo")}, {qn("descricao")}, {qn(status_col)},
                           {qn("softdesk_id")}, {qn("integrado_softdesk")}, {qn("integracao_status")}
                    FROM {qn(chamado_tbl)}
                    WHERE {qn("codigo_helpdesk_api")} = %s
                    LIMIT 1
                    """,
                    [codigo],
                )
                row = cursor.fetchone()
                if not row:
                    return _base_result(
                        ok=False,
                        steps=steps,
                        detail=f"Nenhum registro em {chamado_tbl} com codigo_helpdesk_api={codigo!r}.",
                    )

                cid, cur_titulo, cur_desc, cur_status, cur_sid, cur_integ, cur_int_stat = row
                cur_titulo = cur_titulo or ""
                cur_desc = cur_desc or ""
                cur_status = "" if cur_status is None else str(cur_status).strip()
                cur_integ = bool(cur_integ)
                cur_int_stat = (cur_int_stat or "").strip()

                antes = {
                    "chamado_id": cid,
                    "titulo": (cur_titulo or "")[:120],
                    "descricao_len": len(cur_desc or ""),
                    status_col: cur_status,
                    "softdesk_id": cur_sid,
                    "integrado_softdesk": cur_integ,
                    "integracao_status": cur_int_stat,
                }
                step("7_estado_banco", "Registro atual no banco", **antes)

                novo_status_id: int | None = None
                status_lookup_detail = ""
                matched_needle: str | None = None
                if status_needles and cfg_tbl:
                    novo_status_id, status_lookup_detail, matched_needle = (
                        _lookup_status_chamado_config_first_match(cursor, qn, cfg_tbl, status_needles)
                    )
                elif status_needles and not cfg_tbl:
                    status_lookup_detail = "tabela_status_config_vazia"
                extraction["status_config_id"] = novo_status_id
                extraction["status_lookup_detail"] = status_lookup_detail
                extraction["status_matched_needle"] = matched_needle
                step(
                    "7b_status_config",
                    "Status do JSON → id em tabela de config (codigo_integracao)",
                    status_needles=status_needles,
                    status_id=novo_status_id,
                    needle_usado=matched_needle,
                    detalhe=status_lookup_detail,
                    tabela=cfg_tbl or None,
                )

                cur_status_norm = _coerce_int_id(cur_status)

                updates: list[str] = []
                params: list[Any] = []
                historico_rows: list[tuple[Any, ...]] = []
                changed_fields: list[str] = []

                obs = "Sincronização RetornaDossie (micro-sap)"

                def add_hist(
                    *,
                    st_old: str | None = None,
                    st_new: str | None = None,
                    campo: str | None = None,
                    v_old: str | None = None,
                    v_new: str | None = None,
                ) -> None:
                    historico_rows.append(
                        (cid, st_old, st_new, campo, v_old, v_new, obs[:255])
                    )

                def cmp_field(
                    name: str,
                    *,
                    novo: Any,
                    atual: Any,
                    aplicar: bool,
                    motivo_skip: str | None = None,
                ) -> None:
                    rec: dict[str, Any] = {
                        "campo": name,
                        "valor_banco": atual,
                        "valor_dossie": novo,
                        "acao": "atualizar" if aplicar else "ignorar",
                    }
                    if motivo_skip:
                        rec["motivo"] = motivo_skip
                    comparisons.append(rec)

                if novo_titulo is not None:
                    aplicar = novo_titulo != (cur_titulo or "").strip()
                    cmp_field(
                        "titulo",
                        novo=novo_titulo,
                        atual=cur_titulo,
                        aplicar=aplicar,
                        motivo_skip=None if aplicar else "igual ao banco",
                    )
                    if aplicar:
                        updates.append(f"{qn('titulo')} = %s")
                        params.append(novo_titulo)
                        changed_fields.append("titulo")
                        add_hist(campo="titulo", v_old=cur_titulo[:4000], v_new=novo_titulo[:4000])
                else:
                    comparisons.append(
                        {
                            "campo": "titulo",
                            "valor_banco": (cur_titulo or "")[:120],
                            "valor_dossie": None,
                            "acao": "ignorar",
                            "motivo": "não encontrado no JSON",
                        }
                    )

                if nova_desc is not None:
                    aplicar = nova_desc != (cur_desc or "").strip()
                    cmp_field(
                        "descricao",
                        novo=f"({len(nova_desc)} caracteres)",
                        atual=f"({len(cur_desc or '')} caracteres)",
                        aplicar=aplicar,
                        motivo_skip=None if aplicar else "igual ao banco",
                    )
                    if aplicar:
                        updates.append(f"{qn('descricao')} = %s")
                        params.append(nova_desc)
                        changed_fields.append("descricao")
                        add_hist(campo="descricao", v_old=(cur_desc or "")[:4000], v_new=nova_desc[:4000])
                else:
                    comparisons.append(
                        {
                            "campo": "descricao",
                            "acao": "ignorar",
                            "motivo": "não encontrado no JSON",
                        }
                    )

                if novo_status_id is not None:
                    aplicar = novo_status_id != cur_status_norm
                    cmp_field(
                        status_col,
                        novo=novo_status_id,
                        atual=cur_status,
                        aplicar=aplicar,
                        motivo_skip=None if aplicar else "igual ao banco",
                    )
                    if aplicar:
                        updates.append(f"{qn(status_col)} = %s")
                        params.append(novo_status_id)
                        changed_fields.append(status_col)
                        add_hist(
                            st_old=None if cur_status is None else str(cur_status),
                            st_new=str(novo_status_id),
                        )
                elif status_needles:
                    comparisons.append(
                        {
                            "campo": status_col,
                            "valor_dossie_needles": status_needles,
                            "acao": "ignorar",
                            "motivo": (
                                "nenhum registro com codigo_integracao igual a nenhum candidato do JSON "
                                f"({status_lookup_detail})"
                            ),
                        }
                    )
                else:
                    comparisons.append(
                        {
                            "campo": status_col,
                            "acao": "ignorar",
                            "motivo": "não encontrado no JSON",
                        }
                    )

                if novo_sid is not None:
                    aplicar = novo_sid != cur_sid
                    cmp_field(
                        "softdesk_id",
                        novo=novo_sid,
                        atual=cur_sid,
                        aplicar=aplicar,
                        motivo_skip=None if aplicar else "igual ao banco",
                    )
                    if aplicar:
                        updates.append(f"{qn('softdesk_id')} = %s")
                        params.append(novo_sid)
                        changed_fields.append("softdesk_id")
                        add_hist(
                            campo="softdesk_id",
                            v_old="" if cur_sid is None else str(cur_sid),
                            v_new=str(novo_sid),
                        )
                else:
                    comparisons.append(
                        {
                            "campo": "softdesk_id",
                            "acao": "ignorar",
                            "motivo": "não numérico ou ausente no JSON",
                        }
                    )

                integ_aplicar = not cur_integ or cur_int_stat != "enviado"
                comparisons.append(
                    {
                        "campo": "integracao",
                        "integrado_softdesk_banco": cur_integ,
                        "integracao_status_banco": cur_int_stat,
                        "acao": "atualizar" if integ_aplicar else "ignorar",
                        "motivo": None if integ_aplicar else "já enviado",
                    }
                )
                if integ_aplicar:
                    updates.append(f"{qn('integrado_softdesk')} = %s")
                    params.append(True)
                    updates.append(f"{qn('integracao_status')} = %s")
                    params.append("enviado")
                    changed_fields.extend(["integrado_softdesk", "integracao_status"])
                    add_hist(
                        campo="integracao_status",
                        v_old=cur_int_stat or "—",
                        v_new="enviado",
                    )

                if not updates:
                    step("8_chamado", "Nenhum UPDATE necessário no chamado", historico_inserts=0)
                    atividades_stats = persist_atividades_from_dossie(
                        cursor=cursor,
                        qn=qn,
                        chamado_id=int(cid),
                        inner=inner,
                        dossie=dossie,
                        steps=steps,
                    )
                    return _base_result(
                        ok=True,
                        steps=steps,
                        chamado_id=cid,
                        updated=False,
                        dossie_extraction=extraction,
                        field_comparisons=comparisons,
                        database={
                            "antes": antes,
                            "update_executed": False,
                            "historico_inserts": 0,
                            "atividades": atividades_stats,
                        },
                        detail="Nenhum campo a atualizar em relação ao banco.",
                    )

                updates.append(f"{qn('atualizado_em')} = %s")
                params.append(timezone.now())

                params.append(cid)
                set_clause = ", ".join(updates)
                step(
                    "8_sql_update",
                    "UPDATE helpdesk_chamado",
                    campos=changed_fields + ["atualizado_em"],
                    set_clauses_count=len(updates),
                )
                cursor.execute(
                    f"UPDATE {qn(chamado_tbl)} SET {set_clause} WHERE {qn('id')} = %s",
                    params,
                )

                ins = f"""
                    INSERT INTO {qn(hist_tbl)} (
                        {qn("chamado_id")},
                        {qn("status_anterior")},
                        {qn("status_novo")},
                        {qn("campo")},
                        {qn("valor_antigo")},
                        {qn("valor_novo")},
                        {qn("usuario_id")},
                        {qn("observacao")},
                        {qn("criado_em")}
                    ) VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s)
                """
                now = timezone.now()
                for h in historico_rows:
                    cursor.execute(ins, [*h, now])

                step(
                    "9_historico",
                    "INSERT em helpdesk_chamadohistorico",
                    linhas=len(historico_rows),
                )
                atividades_stats = persist_atividades_from_dossie(
                    cursor=cursor,
                    qn=qn,
                    chamado_id=int(cid),
                    inner=inner,
                    dossie=dossie,
                    steps=steps,
                )
                step(
                    "10_commit",
                    "Transação gravada no PostgreSQL (Conversys)",
                    chamado_id=cid,
                    atividades_resumo={
                        "inseridas": atividades_stats.get("inserted"),
                        "atualizadas": atividades_stats.get("updated"),
                    },
                )

        return _base_result(
            ok=True,
            steps=steps,
            chamado_id=cid,
            updated=True,
            fields=changed_fields,
            changes_count=len(historico_rows),
            dossie_extraction=extraction,
            field_comparisons=comparisons,
            database={
                "antes": antes,
                "update_executed": True,
                "historico_inserts": len(historico_rows),
                "campos_alterados": changed_fields,
                "atividades": atividades_stats,
            },
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("dossie_sync.failed", extra={"codigo": codigo})
        steps.append({"phase": "erro", "message": str(exc)[:500], "data": {"tipo": type(exc).__name__}})
        return _base_result(ok=False, steps=steps, detail=str(exc)[:2000])
