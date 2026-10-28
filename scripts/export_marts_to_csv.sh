#!/usr/bin/env bash
# Exporta las 3 tablas de salida (marts) a CSV en outputs/.
#
# Es un SNAPSHOT para poder inspeccionar las tablas de salida
# sin correr todo el pipeline. La fuente de verdad son los marts en BigQuery;
# regenerar tras cada `dbt build` con:  bash scripts/export_marts_to_csv.sh
#
# Requiere: gcloud/bq autenticado y GCP_PROJECT_ID (o un proyecto por defecto
# en `gcloud config`). El dataset es dbt_dss (target de dbt).

set -euo pipefail

PROJECT="${GCP_PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
if [ -z "$PROJECT" ]; then
  echo "ERROR: definí GCP_PROJECT_ID o un proyecto por defecto (gcloud config set project ...)." >&2
  exit 1
fi

DATASET="dbt_dss"
OUT="$(cd "$(dirname "$0")/.." && pwd)/outputs"
mkdir -p "$OUT"

MARTS=(
  fct_cost_allocated_entity_period   # tabla de salida 1: costo asignado por entidad/período
  fct_cost_unallocated_detail        # tabla de salida 2: costo no asignado, por razón
  fct_unassigned_usage               # diagnóstica: uso sin costo (vCPU-h/GB)
)

for TBL in "${MARTS[@]}"; do
  echo "Exportando ${TBL} ..."
  bq query --project_id="$PROJECT" --use_legacy_sql=false --format=csv --max_rows=1000000 \
    "select * from \`${PROJECT}.${DATASET}.${TBL}\` order by 1, 2" > "${OUT}/${TBL}.csv"
done

echo "Listo → ${OUT}"
