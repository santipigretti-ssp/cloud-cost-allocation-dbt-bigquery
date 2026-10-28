-- Mapa entidad ↔ cluster_set expandido por día: spine de fechas (min/max de
-- billing) × vigencias del map. valid_to es EXCLUSIVO (verificado en datos:
-- app-loyalty-01 cierra cs-065 el 2025-09-01 y abre cs-003 el mismo día —
-- migración limpia sin doble conteo). Una entidad puede tener 2 sets el mismo
-- día (app-fraud-04 durante jun-jul 2025): ambos válidos por decisión de negocio.

with billing_date_range as (

    select
        min(usage_date) as min_date,
        max(usage_date) as max_date
    from {{ ref('stg_billing_lines') }}

),

date_spine as (

    select usage_date
    from billing_date_range
    cross join unnest(generate_date_array(min_date, max_date)) as usage_date

),

entity_map as (

    select * from {{ ref('stg_entity_clusterset_map') }}

)

select distinct
    date_spine.usage_date,
    entity_map.entity_id,
    entity_map.cluster_set_id

from date_spine
inner join entity_map
    on date_spine.usage_date >= entity_map.valid_from
    and date_spine.usage_date < entity_map.valid_to
