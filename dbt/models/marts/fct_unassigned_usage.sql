-- Tabla de salida 3 (diagnóstica): USO que no recibe costo. Grano:
-- billing_period × entity_id × cost_driver × reason. Medida única
-- `usage_amount` (vCPU-hours para compute, GB para storage — el driver da la
-- unidad). NO participa de la reconciliación: es evidencia, no toca dinero.
-- Separada de fct_cost_unallocated_detail a propósito: otra medida, otra
-- unidad, otro grano y otro rol.
--   · orphan_entity_usage    — uso de las 5 entidades sin mapeo a set vigente:
--     generan uso pero no reciben costo (falta el alta en el mapa).
--   · excluded_outlier_usage — filas outlier excluidas del peso de asignación
--     (registro corrupto conservado como evidencia, no eliminado).

-- FALLBACK SANDBOX: igual que las otras marts (particiones 2025 expiran al
-- instante); config incremental de producción documentada en
-- fct_cost_allocated_entity_period.

{{ config(materialized='table') }}

{% set period_filter %}
    {% if is_incremental() and var('billing_period', none) %}
        and billing_period = '{{ var("billing_period") }}'
    {% endif %}
{% endset %}

with usage_union as (

    select * from {{ ref('int_usage_unioned') }}

),

orphan_entity_usage as (

    -- uso sin vigencia entidad↔set ese día: no recibe costo, queda de evidencia
    select
        usage_union.billing_period,
        usage_union.entity_id,
        usage_union.cost_driver,
        'orphan_entity_usage' as reason,
        sum(usage_union.usage_amount) as usage_amount
    from usage_union
    left join {{ ref('int_entity_set_daily') }} as entity_sets
        on entity_sets.entity_id = usage_union.entity_id
        and entity_sets.usage_date = usage_union.usage_date
    where entity_sets.entity_id is null
        {{ period_filter }}
    group by billing_period, entity_id, cost_driver

),

excluded_outlier_usage as (

    select
        billing_period,
        entity_id,
        cost_driver,
        'excluded_outlier_usage' as reason,
        sum(usage_amount) as usage_amount
    from {{ ref('int_usage_flagged') }}
    where is_outlier
        {{ period_filter }}
    group by billing_period, entity_id, cost_driver

),

unioned as (

    select * from orphan_entity_usage
    union all
    select * from excluded_outlier_usage

)

select
    billing_period,
    cast(billing_period || '-01' as date) as billing_month,
    entity_id,
    cost_driver,
    reason,
    usage_amount

from unioned
