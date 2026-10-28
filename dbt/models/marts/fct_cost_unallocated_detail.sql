-- Tabla de salida 2: detalle del COSTO que no se pudo asignar a ninguna
-- entidad. Grano: billing_period × reason × (cluster / set según aplique).
-- Medida única `cost` (USD) — participa de la reconciliación sin filtros.
--   · unmapped_cluster    — clusters facturados sin mapeo a set.
--   · orphan_cluster_set  — sets facturados sin entidades vigentes.
--     (los sets con entidades pero sin uso ya NO caen acá: se asignan por
--      even-split en int_entity_cost_daily, ver even_split_no_usage)
-- El overhead tampoco está acá: es un driver asignado en el mart 1. La
-- evidencia de USO sin asignar (uso huérfano, outliers excluidos) vive en su
-- propia tabla, fct_unassigned_usage — son otra medida (vCPU-h/GB), otro grano
-- (entidad×driver) y otro rol (diagnóstico, no toca dinero).

-- FALLBACK SANDBOX: misma situación que fct_cost_allocated_entity_period —
-- el sandbox expira al instante las particiones con fechas 2025 (límite 60
-- días no removible). Config incremental de producción documentada en ese
-- modelo; acá aplica idéntica.

{{ config(materialized='table') }}

with unallocated_cost as (

    select
        billing_period,
        unallocated_reason as reason,
        cost_driver,
        entity_id,
        cluster_set_id,
        cluster_id,
        sum(cost) as cost
    from {{ ref('int_entity_cost_daily') }}
    where unallocated_reason is not null
        {% if is_incremental() and var('billing_period', none) %}
            and billing_period = '{{ var("billing_period") }}'
        {% endif %}
    group by billing_period, reason, cost_driver, entity_id, cluster_set_id, cluster_id

)

select
    billing_period,
    cast(billing_period || '-01' as date) as billing_month,
    reason,
    cost_driver,
    entity_id,
    cluster_set_id,
    cluster_id,
    cost

from unallocated_cost
