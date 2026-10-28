-- Uso diario válido (ambos drivers, sin outliers): única definición de qué
-- filas de uso cuentan para el pipeline. La consumen int_usage_daily (peso de
-- la asignación) y fct_unassigned_usage (evidencia de uso huérfano) — la
-- exclusión de outliers vive una sola vez, en int_usage_flagged, así los dos
-- consumidores no pueden divergir.

select
    billing_period,
    usage_date,
    entity_id,
    usage_amount,
    cost_driver

from {{ ref('int_usage_flagged') }}
where not is_outlier
