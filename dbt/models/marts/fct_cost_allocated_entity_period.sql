-- Tabla de salida 1: costo asignado por entidad y período.
-- Grano: billing_period × entity_id × cost_driver × allocation_method.
-- Formato largo (tidy): UNA sola medida aditiva `cost`, y cost_driver es un
-- miembro de dimensión con tres valores: compute, storage y overhead.
--   · compute / storage → costo directo tageado, asignado por uso.
--   · overhead → overhead sin tag (DataTransfer/CloudWatch/GuardDuty/Support)
--     prorrateado proporcional al costo directo de cada entidad en el período,
--     con allocation_method='overhead_prorated'. Una fila de overhead por
--     entidad y período.
-- Así "SUM(cost) GROUP BY cost_driver" da los tres slices limpios y el total =
-- fully loaded. Vistas derivadas por FILTRO, no por columnas solapadas:
--   directo = cost_driver in (compute,storage) · fully loaded = todo.
-- El overhead, ya asignado acá, NO aparece en fct_cost_unallocated_detail.
--
-- FALLBACK SANDBOX (verificado empíricamente): la config de producción es
-- incremental insert_overwrite particionado por billing_month con
-- copy_partitions (abajo, comentada). El sandbox de BigQuery impone una
-- expiración de particiones máxima de 60 días NO removible sin billing
-- ("Partition expiration time must be less than 60 days while in sandbox
-- mode"), y como los billing_month son fechas 2025 —más viejas que 60 días—
-- BigQuery expira cada partición en el momento de crearla: la tabla quedaba
-- vacía con el build en verde. Con billing habilitado, restaurar dentro del
-- config() de abajo (sin llaves jinja acá: rompen el re-render del preview):
--
--     materialized='incremental',
--     incremental_strategy='insert_overwrite',
--     partition_by= dict con field 'billing_month', data_type 'date',
--                   granularity 'month', copy_partitions true,
--     on_schema_change='fail'
--
-- El filtro por var('billing_period') para reprocesar un mes puntual queda
-- dormido (is_incremental() es false con materialized='table') y se activa
-- solo al volver a incremental.

{{ config(materialized='table') }}

with allocated as (

    select * from {{ ref('int_entity_cost_daily') }}
    where allocation_method is not null
    {% if is_incremental() and var('billing_period', none) %}
        and billing_period = '{{ var("billing_period") }}'
    {% endif %}

),

-- costo directo (compute/storage) por entidad × driver × método
direct as (

    select
        billing_period,
        entity_id,
        cost_driver,
        allocation_method,
        sum(cost) as cost,
        logical_or(is_corrected_source) as is_corrected_source
    from allocated
    group by billing_period, entity_id, cost_driver, allocation_method

),

untagged_overhead as (

    select
        billing_period,
        sum(cost) as overhead_cost,
        logical_or(is_corrected) as is_corrected_source
    from {{ ref('int_billing_lines_corrected') }}
    where is_untagged
    {% if is_incremental() and var('billing_period', none) %}
        and billing_period = '{{ var("billing_period") }}'
    {% endif %}
    group by billing_period

),

-- base de prorrateo del overhead: costo directo total por entidad y por período
entity_direct_totals as (

    select
        billing_period,
        entity_id,
        sum(cost) as entity_direct_cost
    from direct
    group by billing_period, entity_id

),

period_totals as (

    select
        billing_period,
        sum(cost) as total_direct_cost
    from direct
    group by billing_period

),

-- una fila de overhead por entidad: su porción del overhead sin tag del período
overhead_rows as (

    select
        entity_direct_totals.billing_period,
        entity_direct_totals.entity_id,
        'overhead' as cost_driver,
        'overhead_prorated' as allocation_method,
        coalesce(untagged_overhead.overhead_cost, 0)
            * entity_direct_totals.entity_direct_cost
            / nullif(period_totals.total_direct_cost, 0) as cost,
        coalesce(untagged_overhead.is_corrected_source, false) as is_corrected_source

    from entity_direct_totals
    inner join period_totals
        using (billing_period)
    left join untagged_overhead
        using (billing_period)
    where coalesce(untagged_overhead.overhead_cost, 0) > 0

),

combined as (

    select billing_period, entity_id, cost_driver, allocation_method, cost, is_corrected_source
    from direct

    union all

    select billing_period, entity_id, cost_driver, allocation_method, cost, is_corrected_source
    from overhead_rows

)

select
    billing_period,
    cast(billing_period || '-01' as date) as billing_month,
    entity_id,
    cost_driver,
    allocation_method,
    cost,
    is_corrected_source

from combined
