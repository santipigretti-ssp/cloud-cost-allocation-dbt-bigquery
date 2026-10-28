-- EL TEST CRÍTICO: ningún dólar se pierde ni se duplica, medido sobre las
-- tablas de salida. Por billing_period:
--   SUM(cost asignado: compute + storage + overhead)
--   + SUM(no asignado con costo: unmapped_cluster + orphan_cluster_set)
--   = SUM(facturado corregido)                      (± tolerancia)
-- El overhead se cuenta UNA vez, del lado asignado (cost_driver='overhead');
-- ya no está en la tabla de no-asignado. Medida única `cost`, sin solapes.

with billed as (

    select
        billing_period,
        sum(cost) as total_billed
    from {{ ref('int_billing_lines_corrected') }}
    group by billing_period

),

allocated as (

    select
        billing_period,
        sum(cost) as total_allocated
    from {{ ref('fct_cost_allocated_entity_period') }}
    group by billing_period

),

unallocated as (

    -- tabla ya pura de costo: sin filtro `cost is not null` (el uso sin asignar
    -- vive en fct_unassigned_usage y no participa de la reconciliación)
    select
        billing_period,
        sum(cost) as total_unallocated
    from {{ ref('fct_cost_unallocated_detail') }}
    group by billing_period

)

select
    billed.billing_period,
    billed.total_billed,
    coalesce(allocated.total_allocated, 0) as total_allocated,
    coalesce(unallocated.total_unallocated, 0) as total_unallocated,
    billed.total_billed
        - coalesce(allocated.total_allocated, 0)
        - coalesce(unallocated.total_unallocated, 0) as difference_usd

from billed
left join allocated using (billing_period)
left join unallocated using (billing_period)

where abs(
    billed.total_billed
        - coalesce(allocated.total_allocated, 0)
        - coalesce(unallocated.total_unallocated, 0)
) > {{ var('reconciliation_tolerance') }}
