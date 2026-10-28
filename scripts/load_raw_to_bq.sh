#!/usr/bin/env bash
# Carga los 5 CSVs de datasets/ a BigQuery (dataset raw_dss), con WRITE_TRUNCATE.
# Idempotente: re-ejecutar produce exactamente el mismo estado.
#
# Uso:
#   export GCP_PROJECT_ID=mi-proyecto
#   bash scripts/load_raw_to_bq.sh
#
# Los schemas viven en scripts/bq_schemas/*.json (una sola fuente de verdad,
# compartida con scripts/load_raw_to_bq.py que usa el DAG de Airflow).
# Nunca --autodetect: los account_id traen cero inicial (088420000317) y
# autodetect los tiparía INT64 destruyéndolo. IDs siempre STRING.

set -euo pipefail

: "${GCP_PROJECT_ID:?Error: seteá GCP_PROJECT_ID (export GCP_PROJECT_ID=mi-proyecto)}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATASETS_DIR="${REPO_ROOT}/datasets"
SCHEMAS_DIR="${REPO_ROOT}/scripts/bq_schemas"
BQ_DATASET="raw_dss"

echo ">> Proyecto: ${GCP_PROJECT_ID} · Dataset destino: ${BQ_DATASET}"

# Crear el dataset si no existe (idempotente)
if ! bq --project_id="${GCP_PROJECT_ID}" show --dataset "${BQ_DATASET}" >/dev/null 2>&1; then
  echo ">> Creando dataset ${BQ_DATASET}"
  bq --project_id="${GCP_PROJECT_ID}" mk --dataset --location=US "${BQ_DATASET}"
fi

# La lista de tablas se deriva de los schemas: agregar un CSV nuevo = agregar
# su .json en bq_schemas/ (única fuente de verdad, compartida con el .py)
for schema_file in "${SCHEMAS_DIR}"/*.json; do
  table="$(basename "${schema_file}" .json)"
  echo ">> Cargando ${table}.csv -> ${BQ_DATASET}.${table}"
  bq --project_id="${GCP_PROJECT_ID}" load \
    --replace \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --schema="${schema_file}" \
    "${BQ_DATASET}.${table}" \
    "${DATASETS_DIR}/${table}.csv"
done

echo ">> Carga RAW completa."
