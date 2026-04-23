"""
Django settings for SAP Integration microservice.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "change-me-in-production")
DEBUG = os.environ.get("DJANGO_DEBUG", "false").lower() in ("1", "true", "yes")

ALLOWED_HOSTS = [
    h.strip()
    for h in os.environ.get("DJANGO_ALLOWED_HOSTS", "localhost,127.0.0.1").split(",")
    if h.strip()
]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "logs.apps.LogsConfig",
    "sap_queue.apps.SapQueueConfig",
    "integrations.apps.IntegrationsConfig",
    "historico_sap.apps.HistoricoSapConfig",
    "integration_bus.apps.IntegrationBusConfig",
    "softdesk_sync.apps.SoftdeskSyncConfig",
    "api.apps.ApiConfig",
    "monitoring.apps.MonitoringConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "security.middleware.JWTAuthenticationMiddleware",
]

ROOT_URLCONF = "core.urls"
WSGI_APPLICATION = "core.wsgi.application"
ASGI_APPLICATION = "core.asgi.application"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    }
]

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

LOGIN_REDIRECT_URL = "/admin/"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("POSTGRES_DB", "sap_integration"),
        "USER": os.environ.get("POSTGRES_USER", "sap_integration"),
        "PASSWORD": os.environ.get("POSTGRES_PASSWORD", "sap_integration"),
        "HOST": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
        "PORT": os.environ.get("POSTGRES_PORT", "5432"),
        "CONN_MAX_AGE": int(os.environ.get("POSTGRES_CONN_MAX_AGE", "60")),
    }
}

# Banco do Conversys (ambiente-conversys) — leitura da tabela helpdesk_chamado no painel Softdesk
_conversys_db = os.environ.get("CONVERSYS_POSTGRES_DB", "").strip()
if _conversys_db:
    DATABASES["conversys"] = {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": _conversys_db,
        "USER": os.environ.get(
            "CONVERSYS_POSTGRES_USER", os.environ.get("POSTGRES_USER", "postgres")
        ).strip(),
        "PASSWORD": os.environ.get(
            "CONVERSYS_POSTGRES_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "")
        ).strip(),
        "HOST": os.environ.get(
            "CONVERSYS_POSTGRES_HOST", os.environ.get("POSTGRES_HOST", "127.0.0.1")
        ).strip(),
        "PORT": os.environ.get(
            "CONVERSYS_POSTGRES_PORT", os.environ.get("POSTGRES_PORT", "5432")
        ).strip(),
        "CONN_MAX_AGE": int(os.environ.get("POSTGRES_CONN_MAX_AGE", "60")),
    }

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Modelos em ``historico_sap`` usam o banco ``historico_clientes`` (alias ``conversys`` / CONVERSYS_POSTGRES_*).
DATABASE_ROUTERS = ["historico_sap.router.HistoricoClientesRouter"]

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "security.authentication.BearerJWTAuthentication",
    ],
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.IsAuthenticated",
    ],
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
    ],
    "EXCEPTION_HANDLER": "api.exceptions.custom_exception_handler",
}

# JWT validation (issued by main system)
JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY", SECRET_KEY)
JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
JWT_ISSUER = os.environ.get("JWT_ISSUER", "main-system")
_aud = os.environ.get("JWT_AUDIENCE", "").strip()
JWT_AUDIENCE = _aud or None

# Fernet key for SAP password at rest (32 url-safe base64-encoded bytes)
SAP_ENCRYPTION_KEY = os.environ.get("SAP_ENCRYPTION_KEY", "")

# HTTPS para SAP Service Layer (certificado corporativo / cadeia incompleta)
_sap_ca = os.environ.get("SAP_SSL_CA_BUNDLE", "").strip()
SAP_SSL_CA_BUNDLE = _sap_ca or None
_sap_verify_raw = os.environ.get("SAP_SSL_VERIFY", "true").strip().lower()
SAP_SSL_VERIFY = _sap_verify_raw not in ("0", "false", "no", "off")

# Caminho do Service Layer após o host (ex.: b1s/v2 → POST .../b1s/v2/PurchaseInvoices)
SAP_DEFAULT_B1S_PATH = os.getenv("SAP_DEFAULT_B1S_PATH", "b1s/v2").strip().strip("/")

# Celery (optional / future async processing)
CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE = TIME_ZONE

CELERY_BEAT_SCHEDULE = {
    "softdesk-poll-chamados": {
        "task": "softdesk.poll_chamados",
        "schedule": float(os.environ.get("SOFTDESK_POLL_INTERVAL_SECONDS", "60")),
    },
}

# Cache: LocMem default; set REDIS_CACHE_URL for multi-worker rate limiting + dedup visibility
_redis_cache = os.environ.get("REDIS_CACHE_URL", "").strip()
if _redis_cache.startswith("redis://"):
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.redis.RedisCache",
            "LOCATION": _redis_cache,
        }
    }
else:
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "sap-integration-default",
        }
    }

# Softdesk REST (polling / fake webhook)
# Se SOFTDESK_BASE_URL não vier no .env, usa o mesmo host do dossiê (Soft4 / conversys).
SOFTDESK_DOSSIE_BASE_URL = os.environ.get(
    "SOFTDESK_DOSSIE_BASE_URL", "https://conversys.soft4.com.br"
).strip().rstrip("/")
SOFTDESK_BASE_URL = os.environ.get("SOFTDESK_BASE_URL", "").strip().rstrip("/") or SOFTDESK_DOSSIE_BASE_URL
SOFTDESK_API_TOKEN = os.environ.get("SOFTDESK_API_TOKEN", "").strip()
SOFTDESK_API_KEY = os.environ.get("SOFTDESK_API_KEY", "").strip()
# Opcional: hash só para listagem; senão reutiliza SOFTDESK_DOSSIE_HASH_API no client.
SOFTDESK_LIST_HASH_API = os.environ.get("SOFTDESK_LIST_HASH_API", "").strip()
# Soft4 / conversys costuma expor rotas em api/api.php/... (o dossiê usa api/api.php/chamado).
SOFTDESK_CHAMADOS_PATH = os.environ.get("SOFTDESK_CHAMADOS_PATH", "api/api.php/chamados").strip()
SOFTDESK_HTTP_TIMEOUT_SECONDS = float(os.environ.get("SOFTDESK_HTTP_TIMEOUT_SECONDS", "5"))
SOFTDESK_MAX_REQUESTS_PER_MINUTE = int(os.environ.get("SOFTDESK_MAX_REQUESTS_PER_MINUTE", "60"))
SOFTDESK_RL_CACHE_PREFIX = os.environ.get("SOFTDESK_RL_CACHE_PREFIX", "softdesk:rl")
SOFTDESK_PAGE_SIZE = int(os.environ.get("SOFTDESK_PAGE_SIZE", "50"))
SOFTDESK_MAX_PAGES_PER_RUN = int(os.environ.get("SOFTDESK_MAX_PAGES_PER_RUN", "50"))
SOFTDESK_FIRST_PAGE = int(os.environ.get("SOFTDESK_FIRST_PAGE", "1"))
SOFTDESK_AUTO_INCREMENT_PAGE = os.environ.get("SOFTDESK_AUTO_INCREMENT_PAGE", "true").lower() in (
    "1",
    "true",
    "yes",
)
SOFTDESK_PAGE_QUERY_PARAM = os.environ.get("SOFTDESK_PAGE_QUERY_PARAM", "page")
SOFTDESK_PAGE_SIZE_QUERY_PARAM = os.environ.get("SOFTDESK_PAGE_SIZE_QUERY_PARAM", "page_size")
SOFTDESK_UPDATED_SINCE_QUERY_PARAM = os.environ.get(
    "SOFTDESK_UPDATED_SINCE_QUERY_PARAM", "updated_since"
)
SOFTDESK_LIST_ITEMS_KEY = os.environ.get("SOFTDESK_LIST_ITEMS_KEY", "results")
SOFTDESK_LIST_ITEMS_ALT_KEY = os.environ.get("SOFTDESK_LIST_ITEMS_ALT_KEY", "items")
SOFTDESK_NEXT_URL_KEY = os.environ.get("SOFTDESK_NEXT_URL_KEY", "next")
SOFTDESK_META_KEY = os.environ.get("SOFTDESK_META_KEY", "meta")
SOFTDESK_NEXT_PAGE_JSON_PATH = os.environ.get("SOFTDESK_NEXT_PAGE_JSON_PATH", "")
SOFTDESK_CHAMADO_ID_FIELD = os.environ.get("SOFTDESK_CHAMADO_ID_FIELD", "id")
SOFTDESK_CHAMADO_UPDATED_AT_FIELD = os.environ.get("SOFTDESK_CHAMADO_UPDATED_AT_FIELD", "updated_at")
# Sync UI: filter list rows (field names from Softdesk chamado JSON)
SOFTDESK_CHAMADO_CODIGO_HELPDESK_FIELD = os.environ.get(
    "SOFTDESK_CHAMADO_CODIGO_HELPDESK_FIELD", "codigo_helpdesk_api"
).strip()
# Coluna física em ``helpdesk_chamado`` (Django FK costuma ser ``status_id``).
SOFTDESK_CHAMADO_STATUS_FIELD = os.environ.get("SOFTDESK_CHAMADO_STATUS_FIELD", "status_id").strip()
# Conversys usa status ``FECHADO``; comparação é case-insensitive (valor textual se a coluna for texto).
SOFTDESK_CHAMADO_FECHADO_VALUES = os.environ.get("SOFTDESK_CHAMADO_FECHADO_VALUES", "fechado,FECHADO").strip()
# Quando o status for id numérico (``status_id``): ids a excluir da lista (ex.: ``3,5``).
SOFTDESK_CHAMADO_FECHADO_STATUS_IDS = os.environ.get("SOFTDESK_CHAMADO_FECHADO_STATUS_IDS", "").strip()
SOFTDESK_CHAMADO_TITLE_FIELDS = os.environ.get(
    "SOFTDESK_CHAMADO_TITLE_FIELDS", "titulo,assunto,descricao_resumida,subject"
).strip()
SOFTDESK_SYNC_UI_MAX_PAGES = int(os.environ.get("SOFTDESK_SYNC_UI_MAX_PAGES", "50"))
# Painel Softdesk: ciclo automático (dossiê + banco) para todas as linhas da tabela.
SOFTDESK_SYNC_UI_AUTO_INTERVAL_SECONDS = int(os.environ.get("SOFTDESK_SYNC_UI_AUTO_INTERVAL_SECONDS", "60"))
SOFTDESK_SYNC_UI_AUTO_FIRST_DELAY_SECONDS = int(os.environ.get("SOFTDESK_SYNC_UI_AUTO_FIRST_DELAY_SECONDS", "3"))
SOFTDESK_SYNC_UI_BATCH_MAX = int(os.environ.get("SOFTDESK_SYNC_UI_BATCH_MAX", "200"))
SOFTDESK_SYNC_UI_INTER_DOSSIE_DELAY_MS = int(os.environ.get("SOFTDESK_SYNC_UI_INTER_DOSSIE_DELAY_MS", "200"))
# Dossie (RetornaDossie) — SOFTDESK_DOSSIE_BASE_URL definido acima junto ao fallback de SOFTDESK_BASE_URL
SOFTDESK_DOSSIE_PATH = os.environ.get("SOFTDESK_DOSSIE_PATH", "api/api.php/chamado").strip()
SOFTDESK_DOSSIE_FLAG_PARAM = os.environ.get("SOFTDESK_DOSSIE_FLAG_PARAM", "RetornaDossie").strip()
SOFTDESK_DOSSIE_CHAMADO_PARAM = os.environ.get("SOFTDESK_DOSSIE_CHAMADO_PARAM", "chamado").strip()
SOFTDESK_DOSSIE_TIMEOUT_SECONDS = float(os.environ.get("SOFTDESK_DOSSIE_TIMEOUT_SECONDS", "30"))
# Autenticação da API dossiê (Soft4): header hash-api (mesmo valor do Postman)
SOFTDESK_DOSSIE_HASH_API = os.environ.get("SOFTDESK_DOSSIE_HASH_API", "").strip()
SOFTDESK_DOSSIE_BEARER_TOKEN = os.environ.get("SOFTDESK_DOSSIE_BEARER_TOKEN", "").strip()

# API de chamados do backend Conversys (C:\projetos\ambiente-conversys\historico_clientes_backend)
# Ex.: CONVERSYS_API_BASE_URL=http://127.0.0.1:8000  — sem /api no final; rota padrão api/helpdesk/chamados/
# CONVERSYS_API_JWT = access token SimpleJWT (mesmo Bearer do front após login).
CONVERSYS_API_BASE_URL = os.environ.get("CONVERSYS_API_BASE_URL", "").strip().rstrip("/")
CONVERSYS_API_JWT = os.environ.get("CONVERSYS_API_JWT", "").strip()
CONVERSYS_HELPDESK_CHAMADOS_PATH = os.environ.get(
    "CONVERSYS_HELPDESK_CHAMADOS_PATH", "api/helpdesk/chamados"
).strip()
CONVERSYS_API_TIMEOUT_SECONDS = float(os.environ.get("CONVERSYS_API_TIMEOUT_SECONDS", "30"))
# Tabela Django padrão do model helpdesk.Chamado
CONVERSYS_CHAMADOS_TABLE = os.environ.get("CONVERSYS_CHAMADOS_TABLE", "helpdesk_chamado").strip()
CONVERSYS_CHAMADO_HISTORICO_TABLE = os.environ.get(
    "CONVERSYS_CHAMADO_HISTORICO_TABLE", "helpdesk_chamadohistorico"
).strip()
# Tabela de status (codigo_integracao ↔ id) — ex.: helpdesk_statuschamadoconfig ou statuschamadoconfig
CONVERSYS_STATUS_CHAMADO_CONFIG_TABLE = os.environ.get(
    "CONVERSYS_STATUS_CHAMADO_CONFIG_TABLE", "helpdesk_statuschamadoconfig"
).strip()
CONVERSYS_CHAMADOS_QUERY_LIMIT = int(os.environ.get("CONVERSYS_CHAMADOS_QUERY_LIMIT", "2000"))
CONVERSYS_ATIVIDADE_TABLE = os.environ.get(
    "CONVERSYS_ATIVIDADE_TABLE", "helpdesk_atividadechamado"
).strip()
CONVERSYS_ATENDENTE_TABLE = os.environ.get("CONVERSYS_ATENDENTE_TABLE", "helpdesk_atendente").strip()
CONVERSYS_USER_TABLE = os.environ.get("CONVERSYS_USER_TABLE", "auth_user").strip()
# Mapeamento UsuarioSap.raw_json → tabela CONVERSYS_USER_TABLE (upsert em ``integrations.users_conversys_sync``).
CONVERSYS_USER_COL_USERNAME = os.environ.get("CONVERSYS_USER_COL_USERNAME", "username").strip()
CONVERSYS_USER_COL_FIRST_NAME = os.environ.get("CONVERSYS_USER_COL_FIRST_NAME", "firstname").strip()
CONVERSYS_USER_COL_EMAIL = os.environ.get("CONVERSYS_USER_COL_EMAIL", "email_address").strip()
# Colunas na tabela CONVERSYS_USERPROFILE_TABLE (não na tabela de usuário).
CONVERSYS_USER_COL_DEPT_SAP = os.environ.get(
    "CONVERSYS_USER_COL_DEPT_SAP", "codigo_departamento_sap"
).strip()
CONVERSYS_USER_COL_SAP_INTERNAL = os.environ.get(
    "CONVERSYS_USER_COL_SAP_INTERNAL", "codigo_usuario_sap"
).strip()
CONVERSYS_USER_PK_COLUMN = os.environ.get("CONVERSYS_USER_PK_COLUMN", "id").strip()
# Schema PostgreSQL opcional (quando a tabela não está no ``search_path`` / não é ``public``).
CONVERSYS_USER_TABLE_SCHEMA = os.environ.get("CONVERSYS_USER_TABLE_SCHEMA", "").strip()
# Perfil: ``codigo_departamento_sap`` / ``codigo_usuario_sap`` (mapeamento Department / InternalKey do SAP).
CONVERSYS_USERPROFILE_TABLE = os.environ.get("CONVERSYS_USERPROFILE_TABLE", "userprofiles").strip()
CONVERSYS_USERPROFILE_TABLE_SCHEMA = os.environ.get("CONVERSYS_USERPROFILE_TABLE_SCHEMA", "").strip()
# Ligação perfil ↔ usuário: ``fk`` = coluna separada (ex. user_id); ``shared_pk`` = mesmo ``id`` nas duas tabelas.
# ``auto``: usa FK se existir no banco; senão usa PK compartilhada se ``id`` existir em userprofiles.
CONVERSYS_USERPROFILE_LINK_MODE = os.environ.get("CONVERSYS_USERPROFILE_LINK_MODE", "auto").strip().lower()
# Coluna FK em userprofiles (só usada em modo ``fk``).
CONVERSYS_USERPROFILE_USER_FK_COLUMN = os.environ.get(
    "CONVERSYS_USERPROFILE_USER_FK_COLUMN", "user_id"
).strip()
# JSON: listas de chaves no JSON onde procurar a lista (opcional). Padrão cobre atividades, listaAtividades, etc.
DOSSIE_ATIVIDADE_LIST_KEYS = os.environ.get("DOSSIE_ATIVIDADE_LIST_KEYS", "").strip()
# JSON: aliases extras por coluna, ex.: {"descricao":["meuCampoTexto"]}
DOSSIE_ATIVIDADE_FIELD_MAP_JSON = os.environ.get("DOSSIE_ATIVIDADE_FIELD_MAP_JSON", "").strip()
# Legado: o status do dossiê passou a ser resolvido por CONVERSYS_STATUS_CHAMADO_CONFIG_TABLE (codigo_integracao).
DOSSIE_STATUS_MAP_JSON = os.environ.get("DOSSIE_STATUS_MAP_JSON", "").strip()
# JSON opcional: lista de chaves onde está o objeto do chamado antes do padrão interno, ex.: ["meuRetorno","dados.chamado"]
DOSSIE_CHAMADO_BRANCH_KEYS = os.environ.get("DOSSIE_CHAMADO_BRANCH_KEYS", "").strip()
# Se no futuro o filtro incremental existir no backend, defina true e o nome do query param em SOFTDESK_UPDATED_SINCE_QUERY_PARAM.
CONVERSYS_LIST_SEND_UPDATED_SINCE = os.environ.get("CONVERSYS_LIST_SEND_UPDATED_SINCE", "false").lower() in (
    "1",
    "true",
    "yes",
)

# Payload versioning / metrics hooks (documented defaults)
INTEGRATION_DEFAULT_PAYLOAD_VERSION = os.environ.get("INTEGRATION_PAYLOAD_VERSION", "1")
