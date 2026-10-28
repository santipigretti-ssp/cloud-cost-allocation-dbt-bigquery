-- Uso de storage diario por entidad. Sin dedup por clave: el único duplicado
-- de (entity_id, usage_date) es el par outlier del 2025-07-15 de
-- app-reporting-01 (2.000 GB + 45.000 GB) y ambas filas deben sobrevivir —
-- la detección/exclusión del outlier ocurre en int_usage_flagged.
-- DISTINCT solo como defensa ante futuros duplicados exactos (hoy: cero).

with source as (

    select * from {{ source('raw_dss', 'entity_usage_storage') }}

),

deduplicated as (

    select distinct
        billing_period,
        usage_date,
        entity_id,
        usage_amount,
        usage_unit

    from source

)

select * from deduplicated
