-- Nunca silencioso: si un período tiene overhead sin tag pero costo directo
-- total 0, el prorrateo no tiene sobre quién repartir y el overhead se pierde
-- (no se genera la fila cost_driver='overhead' ni queda en no-asignado). Hoy
-- no ocurre; si un feed futuro trae un período solo con líneas sin tag, este
-- test lo alerta sin frenar el pipeline.

{{ config(severity='warn') }}

with overhead as (

    select
        billing_period,
        sum(cost) as overhead_cost
    from {{ ref('int_billing_lines_corrected') }}
    where is_untagged
    group by billing_period

),

direct as (

    select
        billing_period,
        sum(cost) as total_direct_cost
    from {{ ref('fct_cost_allocated_entity_period') }}
    where cost_driver in ('compute', 'storage')
    group by billing_period

)

select
    overhead.billing_period,
    overhead.overhead_cost,
    coalesce(direct.total_direct_cost, 0) as total_direct_cost

from overhead
left join direct
    using (billing_period)

where overhead.overhead_cost > 0
    and coalesce(direct.total_direct_cost, 0) = 0
