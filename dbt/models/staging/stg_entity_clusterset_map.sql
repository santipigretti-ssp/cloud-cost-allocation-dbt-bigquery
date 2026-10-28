-- Mapa entidad → cluster_set con vigencia. valid_to 9999-12-31 se mantiene
-- como fecha alta (vigencia abierta). Una entidad puede tener 2 vigencias
-- solapadas (app-fraud-04: cs-018 + cs-060) — decisión: ambas válidas.

with source as (

    select * from {{ source('raw_dss', 'entity_clusterset_map') }}

),

renamed as (

    select
        entity_id,
        cluster_set_id,
        valid_from,
        valid_to

    from source

)

select * from renamed
