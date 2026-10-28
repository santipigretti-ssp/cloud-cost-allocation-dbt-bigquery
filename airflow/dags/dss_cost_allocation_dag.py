"""DAG de cost allocation del servicio dss: carga RAW -> dbt build -> summary.

Simple y honesto — 3 tareas con BashOperator sobre el mismo repo montado en
/opt/airflow/repo (ver airflow/docker-compose.yml):

    load_raw_to_bq >> dbt_build >> notify_summary

- dbt_build corre `dbt build`: modelos + tests juntos; si un test crítico
  (p.ej. assert_reconciliation) falla, el DAG falla.
- Trigger manual (schedule=None). El conf {"billing_period": "2025-07"} se
  pasa a dbt como --vars: es un hook para el reprocesamiento incremental de
  producción. En esta versión (sandbox) los marts corren como table (sandbox), así que
  ese filtro queda dormido y el build es COMPLETO — no reprocesa solo ese mes
  (ver comentario en el modelo). La idempotencia se mantiene por full-rebuild.
- En producción esto viviría en Composer con el mismo shape; para este
  volumen alcanzaría Cloud Scheduler o dbt Cloud — el DAG demuestra el fit
  con el stack Composer + dbt + BigQuery.
"""

from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/opt/airflow/repo"

with DAG(
    dag_id="dss_cost_allocation",
    description="Cost allocation dss: RAW -> dbt (staging/intermediate/marts + tests) -> summary",
    schedule=None,
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    tags=["dss", "cost-allocation", "dbt"],
) as dag:

    load_raw_to_bq = BashOperator(
        task_id="load_raw_to_bq",
        bash_command=f"python {REPO}/scripts/load_raw_to_bq.py",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        # una sola interpolación jinja (el billing_period del trigger config,
        # vacío si no vino); el branching lo hace bash — más legible que
        # anidar jinja de Airflow dentro del bash_command
        bash_command=dedent(
            f"""
            set -euo pipefail
            cd {REPO}/dbt
            dbt deps
            BILLING_PERIOD='{{{{ dag_run.conf.get("billing_period", "") }}}}'
            if [ -n "$BILLING_PERIOD" ]; then
                echo "NOTA: billing_period=$BILLING_PERIOD se pasa a dbt --vars. Con los marts en modo incremental (config de producción, comentada en el modelo) reprocesaría solo ese mes; en el sandbox corren como table, así que el build es COMPLETO y el filtro queda dormido."
                dbt build --profiles-dir {REPO}/profiles --vars "{{billing_period: '$BILLING_PERIOD'}}"
            else
                dbt build --profiles-dir {REPO}/profiles
            fi
            """
        ),
    )

    notify_summary = BashOperator(
        task_id="notify_summary",
        bash_command=f"python {REPO}/scripts/print_marts_summary.py",
    )

    load_raw_to_bq >> dbt_build >> notify_summary
