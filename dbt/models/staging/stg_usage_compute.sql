-- Uso de cómputo diario por entidad. Solo SELECT DISTINCT: colapsa copias
-- EXACTAS (el 2025-08-20 vino duplicado completo con valores 100% idénticos,
-- verificado). Igual que stg_usage_storage: la política de duplicados es
-- simétrica entre ambos drivers. Un eventual duplicado de clave con valores
-- EN CONFLICTO NO se resuelve acá con un min() arbitrario — sobrevive a la
-- detección de outliers en int_usage_flagged y, si la regla no lo resuelve,
-- rompe el test estricto de unicidad de int_usage_unioned (decisión humana).

with source as (

    select * from {{ source('raw_dss', 'entity_usage_compute') }}

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
