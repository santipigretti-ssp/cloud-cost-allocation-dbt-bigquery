"""Carga los 5 CSVs de datasets/ a BigQuery (raw_dss), WRITE_TRUNCATE.

Equivalente Python de load_raw_to_bq.sh para entornos sin bq CLI (el
contenedor de Airflow). Comparte los schemas de scripts/bq_schemas/*.json —
una sola fuente de verdad. Nunca autodetect: los account_id traen cero
inicial y deben ser STRING.

Uso:
    export GCP_PROJECT_ID=mi-proyecto
    python scripts/load_raw_to_bq.py
"""

import json
import os
import sys
from pathlib import Path

from google.cloud import bigquery

BQ_DATASET = "raw_dss"
BQ_LOCATION = "US"

REPO_ROOT = Path(__file__).resolve().parent.parent
DATASETS_DIR = REPO_ROOT / "datasets"
SCHEMAS_DIR = REPO_ROOT / "scripts" / "bq_schemas"

# La lista de tablas se deriva de los schemas: agregar un CSV nuevo = agregar
# su .json en bq_schemas/ (única fuente de verdad, compartida con el .sh)
TABLES = sorted(path.stem for path in SCHEMAS_DIR.glob("*.json"))


def main() -> None:
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        sys.exit("Error: seteá GCP_PROJECT_ID (export GCP_PROJECT_ID=mi-proyecto)")

    client = bigquery.Client(project=project_id)

    dataset_ref = bigquery.Dataset(f"{project_id}.{BQ_DATASET}")
    dataset_ref.location = BQ_LOCATION
    client.create_dataset(dataset_ref, exists_ok=True)
    print(f">> Proyecto: {project_id} · Dataset destino: {BQ_DATASET}")

    for table in TABLES:
        schema_fields = json.loads((SCHEMAS_DIR / f"{table}.json").read_text())
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[bigquery.SchemaField.from_api_repr(f) for f in schema_fields],
        )
        csv_path = DATASETS_DIR / f"{table}.csv"
        print(f">> Cargando {csv_path.name} -> {BQ_DATASET}.{table}")
        with open(csv_path, "rb") as f:
            job = client.load_table_from_file(
                f, f"{project_id}.{BQ_DATASET}.{table}", job_config=job_config
            )
        job.result()
        print(f"   {job.output_rows} filas cargadas")

    print(">> Carga RAW completa.")


if __name__ == "__main__":
    main()
