# Cloud Cost Allocation Pipeline (BigQuery + dbt + Airflow)

![dbt](https://img.shields.io/badge/dbt-1.7+-FF694B?logo=dbt&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-Sandbox-4285F4?logo=googlebigquery&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)

> **English summary.** An idempotent ELT pipeline that allocates a cloud service's AWS bill — reported per cluster — to the applications that actually consume it (FinOps chargeback/showback). Raw CSVs are loaded to BigQuery with explicit schemas, transformed in dbt through staging → intermediate → marts, and orchestrated with an Airflow DAG. Highlights:
> - **Every dollar reconciles**: a dbt test enforces *allocated + unallocated = billed* per month (tolerance $0.01).
> - **12 real-world data anomalies** handled explicitly — a ×1000 mis-scaled account (fixed via a versioned *corrections seed*, not hardcoded), untagged lines, unmapped clusters, clusters shared across pools, overlapping validity windows, exact duplicates and a conflicting-value outlier.
> - **Usage-proportional allocation** with flagged fallbacks (even split for idle reserved capacity), overhead modeled as a first-class cost driver in a tidy long format.
> - **Re-runnable & idempotent** (WRITE_TRUNCATE + full rebuild), with business thresholds exposed as dbt `vars`.
> - An executive proposal deck with an interactive dashboard ([`docs/propuesta.html`](docs/propuesta.html)).
>
> Docs are in Spanish; code, models and columns are in English.

Pipeline ELT idempotente que asigna el costo de cloud de un servicio (`dss`, reportado por cluster) a las aplicaciones (entidades) que lo consumen, con detalle auditable de lo que no se puede asignar.

**Caso de estudio con datos sintéticos**: un servicio ficticio con ~120 aplicaciones, 138 clusters y 3 cuentas de AWS, período 2025-04 a 2025-09 (~$249k/mes). Los datos incluyen a propósito las anomalías típicas de un feed de billing real.

## Qué incluye

| Pieza | Dónde |
|---|---|
| **Propuesta ejecutiva + dashboard** | [`docs/propuesta.html`](docs/propuesta.html) (abrir en el navegador) · versión [`docs/propuesta.pdf`](docs/propuesta.pdf) |
| **Código re-ejecutable + tests de calidad** | proyecto dbt (`dbt/`) + DAG de Airflow (`airflow/`) + carga (`scripts/`); el control crítico es `assert_reconciliation` |
| **Tablas de salida** | 3 marts en `dbt_dss`: `fct_cost_allocated_entity_period` + `fct_cost_unallocated_detail` (principales) + `fct_unassigned_usage` (diagnóstica). **Snapshot en CSV: [`outputs/`](outputs/)** — inspeccionables sin correr el pipeline (regenerar con `bash scripts/export_marts_to_csv.sh`) |

Dos vistas del mismo trabajo, según la audiencia:

- **Negocio** → [`docs/propuesta.html`](docs/propuesta.html) (o [PDF](docs/propuesta.pdf)): para el dueño del servicio — método, decisiones, trade-offs, preguntas y el dashboard de costo por entidad/driver.
- **Técnico** → [`docs/resumen_pipeline.html`](docs/resumen_pipeline.html) ([PDF](docs/resumen_pipeline.pdf)): cómo fluye el dato capa por capa y dónde vive cada regla, decisión y test.

## Arquitectura

```
CSVs (datasets/) ──bq load──▶ raw_dss ──dbt──▶ staging ──▶ intermediate ──▶ marts
                              (RAW, sin        (tipado,     (corrección,     (fct_cost_allocated_entity_period,
                               transformar)     1:1, dedup)   asignación)      fct_cost_unallocated_detail,
                                                                              fct_unassigned_usage)
```

Principio de capas: **staging es estrictamente 1:1 con la fuente** (tipado, renombrado, dedup intra-tabla, flags). Toda regla de negocio — corrección de la cuenta 846, detección de outliers, splits de costo — vive en intermediate, donde es explícita y testeable.

## Setup

Requisitos: `gcloud` CLI (incluye `bq`), Python 3.9+, un proyecto de BigQuery (el Sandbox gratuito alcanza).

```bash
# 1. Autenticación
gcloud auth login
gcloud auth application-default login   # credenciales para dbt (method: oauth)

# 2. Proyecto
export GCP_PROJECT_ID=<tu-proyecto>

# 3. Dependencias
pip install -r requirements.txt
cd dbt && dbt deps && cd ..
```

## Cómo correr

```bash
# 1. Cargar los 5 CSVs a BigQuery (dataset raw_dss, WRITE_TRUNCATE = idempotente)
bash scripts/load_raw_to_bq.sh

# 2. Correr dbt (seeds + modelos + tests)
cd dbt
dbt build --profiles-dir ../profiles
```

Re-ejecutar ambos pasos produce exactamente el mismo resultado (idempotencia).

### Vía Airflow (opcional, requiere Docker)

```bash
export GCP_PROJECT_ID=<tu-proyecto>
cd airflow && docker compose up
# UI en http://localhost:8080 (la password de admin la imprime el log al arrancar)
# → triggerear el DAG dss_cost_allocation: load_raw_to_bq >> dbt_build >> notify_summary
```

El DAG usa `scripts/load_raw_to_bq.py` (mismos schemas JSON que el script bash, desde `scripts/bq_schemas/`) porque el contenedor no trae `bq` CLI.

> ⚠️ **Si el repo vive en una carpeta sincronizada (Google Drive/Dropbox/iCloud):** Docker no puede leer archivos que el sync tenga "online-only" (dataless) — falla con `OSError: Resource deadlock avoided` y puede dejar trabado el File Provider (se recupera con `killall fileproviderd`). Antes de levantar Airflow, materializá el repo (`find . -type f ! -path "./.venv/*" -exec cat {} + > /dev/null`) o, mejor, cloná el repo a disco local para correr Docker. El DAG acepta `{"billing_period": "2025-07"}` como *hook* para el reprocesamiento incremental de producción, pero **en esta versión (sandbox) es un no-op**: los marts corren como `table` (por la expiración de particiones del sandbox, ver más abajo), así que el build siempre es completo — no reprocesa solo ese mes. La re-ejecutabilidad se garantiza igual por full-rebuild idempotente. En producción este mismo DAG vive en Composer sin docker-compose; para este volumen también alcanzaría Cloud Scheduler o dbt Cloud — el DAG demuestra el fit con el stack Composer + dbt + BigQuery.

## Decisiones de diseño (carga y staging)

1. **Schemas explícitos en la carga, nunca `--autodetect`.** Los `account_id` traen cero inicial (`088420000317`); autodetect los tiparía INT64 y lo destruiría silenciosamente. Los IDs son STRING por contrato.
2. **Corrección de la cuenta 846 como dato versionado** (`dbt/seeds/cost_corrections.csv`): factor, vigencia, autoría y razón auditables. Se aplica en intermediate, no en staging ni hardcodeada.
3. **Política de duplicados uniforme (compute y storage por igual).** Antes de deduplicar se pregunta qué *es* una fila: (A) copia **exacta** → `SELECT DISTINCT` mecánico (resuelve el 2025-08-20 de compute); (B) misma clave con **valores en conflicto** → nunca se resuelve con un `min()` arbitrario, pasa a la regla de outlier o rompe un test. Los dos feeds de uso tienen forma idéntica, así que reciben el mismo tratamiento — nada del modo de falla es propio de storage.
4. **Detección de outliers simétrica y conservadora.** `int_usage_flagged` calcula, por entidad × driver, la mediana móvil 14d y marca outlier solo si el uso supera 5× la mediana **Y** coexiste otra fila de patrón esa fecha (evidencia de registro corrupto). Un valor alto **aislado** NO se excluye —un pico real debe cobrarse a quien lo generó— pero lo lista `assert_single_row_usage_spikes` para revisión. La exclusión del peso vive una sola vez (`int_usage_unioned`), y su unicidad estricta post-exclusión rompe el build ante cualquier conflicto no resuelto.
5. **Anomalías se flaguean, nunca se borran silenciosamente** (`is_untagged`, `sets_per_cluster`, `is_outlier`, `is_corrected`).
6. **Umbrales de negocio como `vars` de dbt** (multiplicador de outlier, ventana, tolerancia de reconciliación): ajustables sin tocar SQL.

## Tablas de salida

- **`fct_cost_allocated_entity_period`** — grano: período × entidad × cost_driver × allocation_method. Formato largo: **una sola medida `cost`** y `cost_driver ∈ {compute, storage, overhead}` (el overhead sin tag es un driver asignado, prorrateado por costo directo). Directo = `cost_driver in (compute,storage)`; fully loaded = todo. `SUM(cost) GROUP BY cost_driver` da los tres slices directo.
- **`fct_cost_unallocated_detail`** — solo **costo** (USD): `unmapped_cluster` ($54.450) + `orphan_cluster_set` ($60.655). Medida única, participa de la reconciliación sin filtros. El overhead ya no está acá (driver asignado); los sets con entidades pero sin uso tampoco (even-split).
- **`fct_unassigned_usage`** — tabla diagnóstica: **uso** que no recibe costo (vCPU-hours/GB), por entidad y driver: `orphan_entity_usage` (5 entidades sin mapeo) y `excluded_outlier_usage` (45.000 GB). Separada del costo a propósito: otra medida, otra unidad, otro rol (no toca dinero). Costo y uso sin asignar nunca coexisten en una fila — mezclarlos era meter dos facts en una tabla.

## Limitaciones del BigQuery Sandbox

- Las tablas expiran a los **60 días** por defecto (se puede subir `default_table_expiration` del dataset).
- **La expiración de particiones está capada a 60 días y no es removible sin billing.** Consecuencia real (verificada): los marts incrementales particionados por mes con fechas 2025 quedaban vacíos — BigQuery expiraba cada partición al crearla, con el build en verde. Por eso los marts corren como `table` en sandbox; la config incremental de producción (`insert_overwrite` + `copy_partitions`) queda comentada en los modelos.
- Sin streaming inserts ni DML — no afecta este pipeline (batch + WRITE_TRUNCATE + CTAS).

## Decisiones de método (asignación)

1. **Corrección 846 en `int_billing_lines_corrected`**: costo corregido ÷1000 vía seed con vigencia; totales mensuales del servicio quedan en $247.8k–252.1k (referencia de negocio: 200-260k). `cost_original` e `is_corrected` conservados para auditoría.
2. **Outlier de storage excluido del peso, no reemplazado**: mediana móvil 14 días por entidad; 1 sola fila flageada (app-reporting-01, 2025-07-15, 45.000 GB), que no participa de la prorrata pero queda visible.
3. **Clusters compartidos: split proporcional al uso** de cada set por día y driver (fallback 50/50 si ninguno tiene uso — hoy nunca ocurre). Los 6 clusters compartidos ($42.276) se reparten 100% por uso.
4. **Vigencias con `valid_to` exclusivo** (verificado con la migración limpia de app-loyalty-01) y solapamientos válidos (app-fraud-04 pesa en ambos sets jun-jul).
5. **Capacidad reservada ociosa (sin uso) se reparte por even-split, flageada**: `sole_member_no_usage` si el pool tiene 1 entidad (app-pricing-04 en septiembre; también app-buyer-07/app-payments-05 en abril), `even_split_no_usage` si tiene N>1 — misma lógica, sin la discontinuidad de cobrar 100% a una entidad sola pero nada a un pool de dos. Nunca silencioso (`assert_no_usage_allocations_flagged`).
6. **Overhead como driver asignado (formato largo)**: el overhead sin tag ($6.420) se prorratea por costo directo y se reporta como filas `cost_driver='overhead'`, no como columna solapada. Así es graficable como un slice más y desaparece el doble reporte (ya no vive también en la tabla de no-asignado). El supuesto que sostiene el modelo — "sin tag" ⟺ "no es EC2/S3" — lo protege `assert_tag_product_alignment` (rompe el build si un feed futuro los desalinea).
7. **Nada se pierde**: `assert_reconciliation` exige asignado (incluye overhead) + no asignado = facturado corregido, por período, con tolerancia $0.01.

**Hallazgo del pipeline**: cs-058 factura desde el 2025-07-01 pero su única entidad entra en vigencia el 07-16 → $871 sin asignar (`orphan_cluster_set`) esos 15 días. Pregunta abierta al dueño del servicio.

## Preguntas abiertas al dueño del servicio

Detalladas en la [propuesta](docs/propuesta.html); las principales:

1. Confirmar la anomalía ×1000 de la cuenta 088420000846 (¿bug de unidad en el feed?) — la corrección está `pending_owner_validation`.
2. cs-058 facturó 15 días antes de la vigencia de su única app: ¿retro-asignar esos $871 o costo de plataforma?
3. Dueños de los 4 clusters sin set y los 3 sets sin apps (~$19k/mes sin asignar).
4. Las 5 apps con uso sin mapeo: ¿deberían estar en el mapa? ¿Quién lo mantiene?
5. Chargeback oficial: ¿costo directo o fully-loaded?
6. El caso 512/septiembre (facturar con uso cero): ¿capacidad reservada deliberada?
7. Costo compartido en pools: la factura de una app puede moverse por el uso de sus vecinas (~$42k / 6 clusters expuestos). ¿Estabilidad (suavizado) o precisión (prorrateo diario)?
8. Base de prorrateo del overhead ($6,4k): ¿costo directo —como hoy— u otra base (nº de instancias, etc.)?

## Autor

**Santi Pigretti** — Data Engineer · [GitHub](https://github.com/santipigretti-ssp)

## Licencia

[MIT](LICENSE)
