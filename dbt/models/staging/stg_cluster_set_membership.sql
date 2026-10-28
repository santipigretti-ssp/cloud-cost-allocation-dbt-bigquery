-- Mapa cluster → cluster_set. DISTINCT elimina el duplicado exacto
-- cl-088/cs-018. sets_per_cluster identifica los 6 clusters compartidos
-- entre 2 sets, cuyo costo se splitea por uso aguas abajo.

with source as (

    select * from {{ source('raw_dss', 'cluster_set_membership') }}

),

deduplicated as (

    select distinct
        cluster_id,
        cluster_set_id

    from source

),

final as (

    select
        cluster_id,
        cluster_set_id,
        count(*) over (partition by cluster_id) as sets_per_cluster

    from deduplicated

)

select * from final
