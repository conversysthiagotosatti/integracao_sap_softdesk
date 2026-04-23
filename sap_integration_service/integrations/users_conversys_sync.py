"""
Replica ``UsuarioSap`` para **users** e, em seguida, **userprofiles** (sempre nesta ordem).

1. **Tabela de usuários** (``CONVERSYS_USER_TABLE``): só ``UserCode`` → username,
   ``UserName`` → firstname, ``eMail`` → email_address. Obtém-se o ``id`` (PK) ao final
   desta etapa (``RETURNING`` no INSERT ou PK do UPDATE).

2. **``userprofiles``**: campos SAP (``Department``, ``InternalKey``). Ligação ao usuário:
   modo **``fk``** (coluna ex.: ``user_id``) ou **``shared_pk``** (o mesmo ``id`` de ``auth_user``).
   ``CONVERSYS_USERPROFILE_LINK_MODE=auto`` escolhe ``fk`` se a coluna existir; senão ``shared_pk``
   quando houver ``id`` no perfil.

Localização do usuário existente: ``codigo_usuario_sap`` via JOIN ao perfil, ou ``username`` em users.
"""
from __future__ import annotations

import logging
import re
from typing import Any

from django.conf import settings
from django.contrib.auth.hashers import make_password
from django.db import connections, transaction
from django.utils import timezone
from django.utils.crypto import get_random_string

from historico_sap.models import UsuarioSap

logger = logging.getLogger(__name__)

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _qualified_table(qn, schema: str, rel: str) -> str:
    """Identificador SQL ``schema.rel`` (PostgreSQL)."""
    sch = (schema or "").strip()
    if sch and sch not in ("pg_catalog", "information_schema"):
        return f"{qn(sch)}.{qn(rel)}"
    return qn(rel)


def _unique_candidates(*names: str | None) -> list[str]:
    out: list[str] = []
    for raw in names:
        if not raw:
            continue
        t = str(raw).strip()
        if t and t not in out:
            out.append(t)
    return out


def _user_table_candidates(configured: str) -> list[str]:
    t = (configured or "auth_user").strip()
    return _unique_candidates(
        t,
        t.lower(),
        "auth_user",
        "users",
        "Users",
        "user",
    )


def _profile_table_candidates(configured: str) -> list[str]:
    t = (configured or "userprofiles").strip()
    low = t.lower()
    extras: list[str | None] = [
        t,
        t.lower(),
        t.title(),
        "userprofiles",
        "userprofile",
        "UserProfile",
        "UserProfiles",
        "user_profile",
        low[:-1] if low.endswith("s") and len(low) > 2 else None,
        low + "s" if not low.endswith("s") else None,
    ]
    return _unique_candidates(*extras)


def _pg_locate_table(
    cursor,
    *,
    configured: str,
    schema_hint: str | None,
    candidates: list[str],
) -> tuple[str, str] | None:
    """Retorna ``(schema, relname)`` da primeira relação encontrada no PostgreSQL."""
    order = """CASE WHEN n.nspname = current_schema() THEN 0
                    WHEN n.nspname = 'public' THEN 1 ELSE 2 END"""
    hint = (schema_hint or "").strip() or None
    for name in candidates:
        if hint:
            cursor.execute(
                f"""
                SELECT n.nspname::text, c.relname::text
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'p')
                  AND n.nspname = %s
                  AND lower(c.relname) = lower(%s)
                LIMIT 1
                """,
                [hint, name],
            )
            row = cursor.fetchone()
            if row:
                return str(row[0]), str(row[1])
        cursor.execute(
            f"""
            SELECT n.nspname::text, c.relname::text
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND lower(c.relname) = lower(%s)
            ORDER BY {order}, n.nspname, c.relname
            LIMIT 1
            """,
            [name],
        )
        row = cursor.fetchone()
        if row:
            return str(row[0]), str(row[1])
    # Nome parecido (user + profile)
    cursor.execute(
        f"""
        SELECT n.nspname::text, c.relname::text
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND lower(c.relname) LIKE '%%user%%'
          AND lower(c.relname) LIKE '%%profil%%'
        ORDER BY {order}, length(c.relname)
        LIMIT 1
        """,
    )
    row = cursor.fetchone()
    return (str(row[0]), str(row[1])) if row else None


def _pg_table_columns(cursor, schema: str, rel: str) -> set[str]:
    cursor.execute(
        """
        SELECT a.attname::text
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        [schema, rel],
    )
    return {str(r[0]) for r in cursor.fetchall()}


def _diagnose_profile_like_tables(cursor) -> str:
    cursor.execute(
        """
        SELECT n.nspname::text || '.' || c.relname::text
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND (
            lower(c.relname) LIKE '%profile%'
            OR lower(c.relname) LIKE '%usuario%'
            OR lower(c.relname) LIKE '%user%'
          )
        ORDER BY n.nspname, c.relname
        LIMIT 20
        """,
    )
    rows = [r[0] for r in cursor.fetchall()]
    return ", ".join(rows) if rows else "(nenhuma tabela parecida encontrada)"


def _table_columns(
    cursor,
    table_setting: str,
    *,
    using: str = "conversys",
    schema_hint: str | None = None,
    candidates: list[str],
) -> tuple[set[str], str, str]:
    """
    Colunas + ``(schema, rel)`` físicos. Só PostgreSQL ``conversys`` com introspecção ``pg_catalog``.
    """
    if connections[using].vendor != "postgresql":
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            """,
            [table_setting],
        )
        cols = {r[0] for r in cursor.fetchall()}
        return cols, "public", table_setting

    loc = _pg_locate_table(
        cursor,
        configured=table_setting,
        schema_hint=schema_hint,
        candidates=candidates,
    )
    if not loc:
        return set(), "", ""
    schema, rel = loc
    cols = _pg_table_columns(cursor, schema, rel)
    return cols, schema, rel


def _detect_profile_fk_column(
    cursor,
    profile_schema: str,
    profile_rel: str,
    user_schema: str,
    user_rel: str,
    *,
    using: str = "conversys",
) -> str | None:
    """Descobre coluna FK em ``profile_rel`` referenciando ``user_rel`` (mesmos nomes do ``pg_catalog``)."""
    if connections[using].vendor != "postgresql":
        return None
    cursor.execute(
        """
        SELECT a.attname::text AS fk_column
        FROM pg_constraint c
        JOIN pg_class tbl ON tbl.oid = c.conrelid
        JOIN pg_namespace tn ON tn.oid = tbl.relnamespace
        JOIN pg_class ref ON ref.oid = c.confrelid
        JOIN pg_namespace rn ON rn.oid = ref.relnamespace
        JOIN unnest(c.conkey) WITH ORDINALITY AS conkey(attnum, ord) ON TRUE
        JOIN pg_attribute a ON a.attrelid = c.conrelid
            AND a.attnum = conkey.attnum
            AND NOT a.attisdropped
        WHERE c.contype = 'f'
          AND tbl.relname = %s AND ref.relname = %s
          AND tn.nspname = %s AND rn.nspname = %s
        ORDER BY conkey.ord
        LIMIT 1
        """,
        [profile_rel, user_rel, profile_schema, user_schema],
    )
    row = cursor.fetchone()
    return str(row[0]) if row else None


def _resolve_profile_link(
    cursor,
    *,
    profile_table_label: str,
    profile_schema: str,
    profile_rel: str,
    user_schema: str,
    user_rel: str,
    profile_cols: set[str],
    pk_col: str,
    col_dept: str,
    col_sap_internal: str,
    configured_fk: str,
    using: str = "conversys",
) -> tuple[str, str]:
    """
    Retorna ``(profile_join_col, link_mode)`` com ``link_mode`` em ``(fk, shared_pk)``.

    - ``fk``: ``p.profile_join_col = u.<pk>`` (ex.: ``user_id`` → ``auth_user.id``).
    - ``shared_pk``: ``p.id = u.id`` (mesmo identificador nas duas tabelas).
    """
    mode_setting = getattr(settings, "CONVERSYS_USERPROFILE_LINK_MODE", "auto").strip().lower()

    if mode_setting == "shared_pk":
        if pk_col not in profile_cols:
            raise RuntimeError(
                f"CONVERSYS_USERPROFILE_LINK_MODE=shared_pk exige a coluna {pk_col!r} em {profile_table_label!r}."
            )
        return pk_col, "shared_pk"

    if mode_setting == "fk":
        fk = (configured_fk or "user_id").strip()
        if fk not in profile_cols:
            raise RuntimeError(
                f"CONVERSYS_USERPROFILE_LINK_MODE=fk: coluna {fk!r} não existe em {profile_table_label!r}. "
                f"Colunas encontradas: {', '.join(sorted(profile_cols)[:50])}"
            )
        return fk, "fk"

    # --- auto ---
    if configured_fk and configured_fk in profile_cols:
        return configured_fk, "fk"

    detected = _detect_profile_fk_column(
        cursor,
        profile_schema,
        profile_rel,
        user_schema,
        user_rel,
        using=using,
    )
    if detected and detected in profile_cols:
        logger.info(
            "users_conversys_sync.profile_fk_auto",
            extra={"profile_table": profile_table_label, "fk_column": detected},
        )
        return detected, "fk"

    for alt in (
        "usuario_id",
        "UsuarioId",
        "usuarioId",
        "fk_user_id",
        "fk_user",
        "auth_user_id",
        "users_id",
        "id_usuario",
        "user_ref_id",
        "userid",
        "userId",
        "user_id",
    ):
        if alt in profile_cols:
            logger.info(
                "users_conversys_sync.profile_fk_fallback",
                extra={"profile_table": profile_table_label, "fk_column": alt},
            )
            return alt, "fk"

    if pk_col in profile_cols and (
        (col_dept and col_dept in profile_cols) or (col_sap_internal and col_sap_internal in profile_cols)
    ):
        logger.info(
            "users_conversys_sync.profile_link_shared_pk_auto",
            extra={"profile_table": profile_table_label, "join_col": pk_col},
        )
        return pk_col, "shared_pk"

    sample = ", ".join(sorted(profile_cols)[:50]) or "(nenhuma coluna: confira nome da tabela e schema)"
    raise RuntimeError(
        f"Não foi possível ligar {profile_table_label!r} ao usuário ({user_schema}.{user_rel}). "
        "Defina CONVERSYS_USERPROFILE_LINK_MODE=fk e CONVERSYS_USERPROFILE_USER_FK_COLUMN, "
        f"ou CONVERSYS_USERPROFILE_LINK_MODE=shared_pk se o ``id`` for o mesmo nas duas tabelas. "
        f"Colunas em {profile_schema}.{profile_rel}: {sample}"
    )


def _dept_sap_code(val: Any) -> int | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val) if val == int(val) else None
    if isinstance(val, dict):
        for k in ("Code", "code", "DepartmentID", "departmentId", "Value", "value"):
            if k in val and val[k] is not None:
                return _dept_sap_code(val[k])
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _str_val(v: Any, max_len: int) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return s[:max_len] if max_len else s


def _int_val(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _row_payload_from_raw_json(rj: dict[str, Any]) -> dict[str, Any]:
    return {
        "username": _str_val(rj.get("UserCode"), 150),
        "firstname": _str_val(rj.get("UserName"), 255),
        "email": _str_val(rj.get("eMail"), 254),
        "dept_sap": _dept_sap_code(rj.get("Department")),
        "sap_internal": _int_val(rj.get("InternalKey")),
    }


def _user_table_vals(
    logical: dict[str, Any],
    *,
    col_username: str,
    col_firstname: str,
    col_email: str,
    user_cols: set[str],
) -> dict[str, Any]:
    mapping = [
        (col_username, logical["username"]),
        (col_firstname, logical["firstname"]),
        (col_email, logical["email"] or ""),
    ]
    out: dict[str, Any] = {}
    for col, val in mapping:
        if col and col in user_cols:
            out[col] = val
    return out


def _profile_table_vals(
    logical: dict[str, Any],
    *,
    col_dept: str,
    col_sap_internal: str,
    profile_cols: set[str],
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if col_dept and col_dept in profile_cols:
        out[col_dept] = logical["dept_sap"]
    if col_sap_internal and col_sap_internal in profile_cols:
        out[col_sap_internal] = logical["sap_internal"]
    return out


def _find_existing_user_pk(
    cursor,
    qn,
    *,
    user_sql: str,
    profile_sql: str,
    pk_col: str,
    col_username: str,
    profile_join_col: str,
    col_sap_internal: str,
    sap_internal: int | None,
    username: str,
    profile_cols: set[str],
) -> int | None:
    """PK do usuário já existente (``codigo_usuario_sap`` só em ``userprofiles``)."""
    if (
        profile_sql
        and profile_join_col in profile_cols
        and col_sap_internal
        and col_sap_internal in profile_cols
        and sap_internal is not None
    ):
        cursor.execute(
            f"""
            SELECT u.{qn(pk_col)}
            FROM {user_sql} u
            INNER JOIN {profile_sql} p ON p.{qn(profile_join_col)} = u.{qn(pk_col)}
            WHERE p.{qn(col_sap_internal)} = %s
            LIMIT 1
            """,
            [sap_internal],
        )
        row = cursor.fetchone()
        if row:
            return int(row[0])
    if col_username and username:
        cursor.execute(
            f"SELECT {qn(pk_col)} FROM {user_sql} WHERE {qn(col_username)} = %s LIMIT 1",
            [username],
        )
        row = cursor.fetchone()
        if row:
            return int(row[0])
    return None


def _autofill_user_insert(vals: dict[str, Any], user_cols: set[str]) -> None:
    if "password" in user_cols and "password" not in vals:
        vals["password"] = make_password(get_random_string(32))
    if "is_active" in user_cols and "is_active" not in vals:
        vals["is_active"] = True
    if "is_staff" in user_cols and "is_staff" not in vals:
        vals["is_staff"] = False
    if "is_superuser" in user_cols and "is_superuser" not in vals:
        vals["is_superuser"] = False
    if "date_joined" in user_cols and "date_joined" not in vals:
        vals["date_joined"] = timezone.now()
    if "last_login" in user_cols and "last_login" not in vals:
        vals["last_login"] = None
    if "last_name" in user_cols and "last_name" not in vals:
        vals["last_name"] = ""


def _integrate_users_table_only(
    cursor,
    qn,
    *,
    user_sql: str,
    profile_sql: str,
    pk_col: str,
    col_username: str,
    profile_join_col: str,
    col_sap_internal: str,
    logical: dict[str, Any],
    user_vals: dict[str, Any],
    user_cols: set[str],
    profile_cols: set[str],
) -> tuple[int, bool]:
    """
    Etapa 1: apenas ``CONVERSYS_USER_TABLE``. Retorna ``(user_pk, criado_novo)``.
    Não grava em ``userprofiles``.
    """
    existing_pk = _find_existing_user_pk(
        cursor,
        qn,
        user_sql=user_sql,
        profile_sql=profile_sql,
        pk_col=pk_col,
        col_username=col_username,
        profile_join_col=profile_join_col,
        col_sap_internal=col_sap_internal,
        sap_internal=logical["sap_internal"],
        username=logical["username"],
        profile_cols=profile_cols,
    )

    if existing_pk is not None:
        assignments = [f"{qn(c)} = %s" for c in user_vals]
        cursor.execute(
            f"UPDATE {user_sql} SET {', '.join(assignments)} WHERE {qn(pk_col)} = %s",
            list(user_vals.values()) + [existing_pk],
        )
        return existing_pk, False

    ins_vals = dict(user_vals)
    _autofill_user_insert(ins_vals, user_cols)
    cols = [c for c in ins_vals if c in user_cols]
    placeholders = ", ".join(["%s"] * len(cols))
    col_sql = ", ".join(qn(c) for c in cols)
    cursor.execute(
        f"INSERT INTO {user_sql} ({col_sql}) VALUES ({placeholders}) RETURNING {qn(pk_col)}",
        [ins_vals[c] for c in cols],
    )
    row = cursor.fetchone()
    if not row:
        raise RuntimeError("INSERT na tabela de usuários não retornou PK.")
    return int(row[0]), True


def _integrate_userprofiles_only(
    cursor,
    qn,
    *,
    profile_sql: str,
    profile_join_col: str,
    user_pk: int,
    profile_vals: dict[str, Any],
    profile_cols: set[str],
) -> None:
    """
    Etapa 2: ``userprofiles`` com ``user_pk`` vindo da etapa 1.
    ``profile_join_col`` é a FK (ex. ``user_id``) ou ``id`` no modo PK compartilhada.
    """
    if not profile_sql or not profile_vals:
        return
    pv = {k: v for k, v in profile_vals.items() if k in profile_cols and k != profile_join_col}
    if not pv:
        return
    if profile_join_col not in profile_cols:
        return
    cursor.execute(
        f"SELECT 1 FROM {profile_sql} WHERE {qn(profile_join_col)} = %s LIMIT 1",
        [user_pk],
    )
    if cursor.fetchone():
        sets = [f"{qn(c)} = %s" for c in pv]
        cursor.execute(
            f"UPDATE {profile_sql} SET {', '.join(sets)} WHERE {qn(profile_join_col)} = %s",
            list(pv.values()) + [user_pk],
        )
    else:
        ins = {profile_join_col: user_pk, **pv}
        cols = [c for c in ins if c in profile_cols]
        placeholders = ", ".join(["%s"] * len(cols))
        col_sql = ", ".join(qn(c) for c in cols)
        cursor.execute(
            f"INSERT INTO {profile_sql} ({col_sql}) VALUES ({placeholders})",
            [ins[c] for c in cols],
        )


@transaction.atomic(using="conversys")
def sync_usuario_sap_to_conversys_users(company_id: str) -> dict[str, Any]:
    cid = (company_id or "").strip()
    if not cid:
        raise ValueError("company_id é obrigatório.")
    if "conversys" not in settings.DATABASES:
        raise RuntimeError("Configure CONVERSYS_POSTGRES_DB para sincronizar usuários.")

    user_table = (getattr(settings, "CONVERSYS_USER_TABLE", "auth_user") or "").strip()
    if not user_table or not _TABLE_RE.match(user_table):
        raise ValueError("CONVERSYS_USER_TABLE inválido.")

    profile_table = (getattr(settings, "CONVERSYS_USERPROFILE_TABLE", "userprofiles") or "").strip()
    if not profile_table or not _TABLE_RE.match(profile_table):
        raise ValueError("CONVERSYS_USERPROFILE_TABLE inválido (ex.: userprofiles).")

    profile_fk_raw = getattr(settings, "CONVERSYS_USERPROFILE_USER_FK_COLUMN", "user_id").strip() or "user_id"

    col_username = getattr(settings, "CONVERSYS_USER_COL_USERNAME", "username").strip()
    col_firstname = getattr(settings, "CONVERSYS_USER_COL_FIRST_NAME", "firstname").strip()
    col_email = getattr(settings, "CONVERSYS_USER_COL_EMAIL", "email_address").strip()
    col_dept = getattr(settings, "CONVERSYS_USER_COL_DEPT_SAP", "codigo_departamento_sap").strip()
    col_sap_internal = getattr(settings, "CONVERSYS_USER_COL_SAP_INTERNAL", "codigo_usuario_sap").strip()
    pk_col = getattr(settings, "CONVERSYS_USER_PK_COLUMN", "id").strip() or "id"

    conn = connections["conversys"]
    qn = conn.ops.quote_name

    inserted = 0
    updated = 0
    skipped = 0
    errors: list[str] = []

    user_schema_hint = getattr(settings, "CONVERSYS_USER_TABLE_SCHEMA", "").strip() or None
    profile_schema_hint = getattr(settings, "CONVERSYS_USERPROFILE_TABLE_SCHEMA", "").strip() or None

    with conn.cursor() as cursor:
        user_cols, us_s, us_r = _table_columns(
            cursor,
            user_table,
            using="conversys",
            schema_hint=user_schema_hint,
            candidates=_user_table_candidates(user_table),
        )
        if not user_cols or not us_r:
            hint = _diagnose_profile_like_tables(cursor)
            raise RuntimeError(
                f"Tabela de usuário «{user_table}» não encontrada no PostgreSQL (conversys). "
                f"Tabelas com 'user'/'profile': {hint}. Ajuste CONVERSYS_USER_TABLE ou CONVERSYS_USER_TABLE_SCHEMA."
            )

        profile_cols, ps_s, ps_r = _table_columns(
            cursor,
            profile_table,
            using="conversys",
            schema_hint=profile_schema_hint,
            candidates=_profile_table_candidates(profile_table),
        )
        if not profile_cols or not ps_r:
            hint = _diagnose_profile_like_tables(cursor)
            raise RuntimeError(
                f"Tabela de perfil «{profile_table}» não encontrada no PostgreSQL (conversys). "
                f"Tabelas parecidas: {hint}. Ajuste CONVERSYS_USERPROFILE_TABLE ou CONVERSYS_USERPROFILE_TABLE_SCHEMA."
            )

        user_ref = _qualified_table(qn, us_s, us_r)
        profile_ref = _qualified_table(qn, ps_s, ps_r)
        logger.info(
            "users_conversys_sync.tables_resolved",
            extra={"user": user_ref, "profile": profile_ref},
        )

        if col_username and col_username not in user_cols:
            raise RuntimeError(
                f"Coluna {col_username!r} não existe em {us_s}.{us_r}. Ajuste CONVERSYS_USER_COL_USERNAME."
            )

        profile_join_col, profile_link_mode = _resolve_profile_link(
            cursor,
            profile_table_label=profile_table,
            profile_schema=ps_s,
            profile_rel=ps_r,
            user_schema=us_s,
            user_rel=us_r,
            profile_cols=profile_cols,
            pk_col=pk_col,
            col_dept=col_dept,
            col_sap_internal=col_sap_internal,
            configured_fk=profile_fk_raw,
            using="conversys",
        )

        qs = UsuarioSap.objects.filter(company_id=cid).order_by("id")
        for u in qs.iterator():
            rj = u.raw_json if isinstance(u.raw_json, dict) else {}
            logical = _row_payload_from_raw_json(rj)
            if not logical["username"]:
                skipped += 1
                errors.append(f"id usuarios_sap={u.id}: UserCode/username vazio")
                continue

            user_vals = _user_table_vals(
                logical,
                col_username=col_username,
                col_firstname=col_firstname,
                col_email=col_email,
                user_cols=user_cols,
            )
            profile_vals = _profile_table_vals(
                logical,
                col_dept=col_dept,
                col_sap_internal=col_sap_internal,
                profile_cols=profile_cols,
            )

            if not user_vals:
                skipped += 1
                continue

            # Savepoint por linha: no PostgreSQL, um erro aborta a transação inteira;
            # sem rollback parcial, os próximos comandos falham até o fim do atomic.
            sid = transaction.savepoint(using="conversys")
            try:
                # --- Etapa 1: somente tabela de usuários; captura PK ---
                user_pk, created_user = _integrate_users_table_only(
                    cursor,
                    qn,
                    user_sql=user_ref,
                    profile_sql=profile_ref,
                    pk_col=pk_col,
                    col_username=col_username,
                    profile_join_col=profile_join_col,
                    col_sap_internal=col_sap_internal,
                    logical=logical,
                    user_vals=user_vals,
                    user_cols=user_cols,
                    profile_cols=profile_cols,
                )
                if created_user:
                    inserted += 1
                else:
                    updated += 1

                # --- Etapa 2: userprofiles com o id obtido acima ---
                _integrate_userprofiles_only(
                    cursor,
                    qn,
                    profile_sql=profile_ref,
                    profile_join_col=profile_join_col,
                    user_pk=user_pk,
                    profile_vals=profile_vals,
                    profile_cols=profile_cols,
                )
                transaction.savepoint_commit(sid, using="conversys")
            except Exception as exc:  # noqa: BLE001
                transaction.savepoint_rollback(sid, using="conversys")
                logger.exception(
                    "users_conversys_sync.row_failed",
                    extra={"usuario_sap_id": u.id, "username": logical["username"]},
                )
                errors.append(f"usuarios_sap id={u.id} {logical['username']!r}: {exc}"[:400])
                skipped += 1

    logger.info(
        "users_conversys_sync.done",
        extra={"company_id": cid, "inserted": inserted, "updated": updated, "skipped": skipped},
    )
    return {
        "ok": True,
        "company_id": cid,
        "user_table": user_table,
        "user_table_resolved": f"{us_s}.{us_r}",
        "userprofile_table": profile_table,
        "userprofile_table_resolved": f"{ps_s}.{ps_r}",
        "userprofile_link": f"{profile_join_col} ({profile_link_mode})",
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "errors_sample": errors[:15],
    }
