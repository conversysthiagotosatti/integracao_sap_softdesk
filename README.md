# Integração SAP / Softdesk (micro-sap)

Serviço Django em `sap_integration_service` com painel de monitoramento, sincronização do dossiê Soft4 (`RetornaDossie`) para o PostgreSQL do Conversys e ciclo automático na UI Softdesk.

## Requisitos

- Python 3.11+ (recomendado)
- PostgreSQL (Conversys) quando usar sync do painel

## Configuração

1. Crie um virtualenv e instale dependências a partir do projeto Django em `sap_integration_service`.
2. Copie variáveis de ambiente para `sap_integration_service/.env` (não versionado). Use hashes, URLs e credenciais reais apenas localmente ou em CI secreto.
3. Ajuste `DJANGO_SETTINGS_MODULE` para `core.settings` ao rodar `manage.py` a partir de `sap_integration_service`.

## Repositório

Código-fonte: [integracao_sap_softdesk](https://github.com/conversysthiagotosatti/integracao_sap_softdesk).

## Licença

Defina a licença no repositório conforme a política da sua organização.
