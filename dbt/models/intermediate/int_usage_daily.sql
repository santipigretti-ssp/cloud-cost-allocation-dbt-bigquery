-- Uso diario por (entidad, cluster_set, cost_driver): uso unificado
-- (int_usage_unioned) asignado a los sets vigentes de cada entidad.
-- El inner join descarta el uso de las 5 entidades huérfanas (sin mapeo a
-- set); ese uso se reporta en fct_unassigned_usage como evidencia, no acá.
-- Una entidad con 2 sets vigentes (fraud-04) pesa en ambos.

with usage_union as (

    select * from {{ ref('int_usage_unioned') }}

),

entity_sets as (

    select * from {{ ref('int_entity_set_daily') }}

)

select
    usage_union.usage_date,
    usage_union.entity_id,
    entity_sets.cluster_set_id,
    usage_union.cost_driver,
    sum(usage_union.usage_amount) as usage_amount

from usage_union
inner join entity_sets
    on usage_union.entity_id = entity_sets.entity_id
    and usage_union.usage_date = entity_sets.usage_date

group by
    usage_union.usage_date,
    usage_union.entity_id,
    entity_sets.cluster_set_id,
    usage_union.cost_driver
