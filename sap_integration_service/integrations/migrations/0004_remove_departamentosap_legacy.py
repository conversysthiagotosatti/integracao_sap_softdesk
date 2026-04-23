# Remove DepartamentoSap do app integrations (tabela passa a historico_sap / banco conversys).

from django.db import migrations


def _drop_legacy_table_if_exists(apps, schema_editor) -> None:
    if schema_editor.connection.alias != "default":
        return
    qn = schema_editor.connection.ops.quote_name
    tbl = qn("departamentossap")
    with schema_editor.connection.cursor() as cursor:
        if schema_editor.connection.vendor == "postgresql":
            cursor.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")
        else:
            cursor.execute(f"DROP TABLE IF EXISTS {tbl}")


def _noop(apps, schema_editor) -> None:
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("integrations", "0003_departamentos_sap"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.DeleteModel(name="DepartamentoSap"),
            ],
            database_operations=[
                migrations.RunPython(_drop_legacy_table_if_exists, _noop),
            ],
        ),
    ]
