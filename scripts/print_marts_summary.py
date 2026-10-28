"""Imprime al log un resumen por período de las 2 tablas de salida.

Placeholder honesto de una notificación real (Slack/email): la tarea
notify_summary del DAG lo ejecuta al final de cada corrida para dejar los
totales auditables en el log de Airflow.

Uso:
    export GCP_PROJECT_ID=mi-proyecto
    python scripts/print_marts_summary.py
"""

import os
import sys

from google.cloud import bigquery

QUERY = """
with allocated as (
    select
        billing_period,
        -- formato largo: directo = compute+storage, fully = todo (incl overhead)
        round(sum(if(cost_driver in ('compute', 'storage'), cost, 0)), 2) as direct_cost,
        round(sum(cost), 2) as fully_loaded_cost,
        count(distinct entity_id) as entities
    from dbt_dss.fct_cost_allocated_entity_period
    group by billing_period
),

unallocated as (
    select
        billing_period,
        round(sum(cost), 2) as unallocated_cost
    from dbt_dss.fct_cost_unallocated_detail
    group by billing_period
)

select
    allocated.billing_period,
    allocated.entities,
    allocated.direct_cost,
    allocated.fully_loaded_cost,
    coalesce(unallocated.unallocated_cost, 0) as unallocated_cost
from allocated
left join unallocated using (billing_period)
order by allocated.billing_period
"""


def main() -> None:
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        sys.exit("Error: seteá GCP_PROJECT_ID")

    client = bigquery.Client(project=project_id)
    rows = list(client.query(QUERY).result())

    print(f"{'período':<10}{'entidades':>10}{'direct':>14}{'fully_loaded':>14}{'sin asignar':>13}")
    for r in rows:
        print(
            f"{r.billing_period:<10}{r.entities:>10}{r.direct_cost:>14,.2f}"
            f"{r.fully_loaded_cost:>14,.2f}{r.unallocated_cost:>13,.2f}"
        )


if __name__ == "__main__":
    main()
