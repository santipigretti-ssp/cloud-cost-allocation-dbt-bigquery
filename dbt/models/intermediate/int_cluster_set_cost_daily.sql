-- Costo del cluster repartido a sus cluster_sets:
--   · 1 set → 100% (split_method = 'full')
--   · 2 sets → proporcional al uso agregado de cada set ese día para ese
--     driver ('usage_split'); si ningún set tiene uso → 50/50
--     (fallback var 'shared_cluster_fallback')
--   · sin set (cl-001/041/042/114) → cluster_set_id NULL con
--     unallocated_reason = 'unmapped_cluster', pasa entero a no asignado.

with cluster_cost as (

    select * from {{ ref('int_cluster_cost_daily') }}

),

membership as (

    select * from {{ ref('stg_cluster_set_membership') }}

),

set_usage as (

    select
        usage_date,
        cluster_set_id,
        cost_driver,
        sum(usage_amount) as set_usage
    from {{ ref('int_usage_daily') }}
    group by usage_date, cluster_set_id, cost_driver

),

-- una fila por (costo de cluster × set candidato); sin membership queda 1 fila con set NULL
candidates as (

    select
        cluster_cost.billing_period,
        cluster_cost.usage_date,
        cluster_cost.account_id,
        cluster_cost.cluster_id,
        cluster_cost.cost_driver,
        cluster_cost.cost,
        cluster_cost.is_corrected_source,
        membership.cluster_set_id,
        membership.sets_per_cluster

    from cluster_cost
    left join membership
        on cluster_cost.cluster_id = membership.cluster_id

),

weighted as (

    select
        candidates.*,
        coalesce(set_usage.set_usage, 0) as set_usage,
        sum(coalesce(set_usage.set_usage, 0)) over (
            partition by
                candidates.cluster_id,
                candidates.account_id,
                candidates.usage_date,
                candidates.cost_driver
        ) as total_usage_across_sets

    from candidates
    left join set_usage
        on set_usage.cluster_set_id = candidates.cluster_set_id
        and set_usage.usage_date = candidates.usage_date
        and set_usage.cost_driver = candidates.cost_driver

)

select
    billing_period,
    usage_date,
    account_id,
    cluster_id,
    cluster_set_id,
    cost_driver,
    is_corrected_source,
    case
        when cluster_set_id is null then cost
        when sets_per_cluster = 1 then cost
        when total_usage_across_sets > 0 then cost * set_usage / total_usage_across_sets
        else cost / sets_per_cluster
    end as cost,
    case
        when cluster_set_id is null then null
        when sets_per_cluster = 1 then 'full'
        when total_usage_across_sets > 0 then 'usage_split'
        else '{{ var("shared_cluster_fallback") }}'
    end as split_method,
    case
        when cluster_set_id is null then 'unmapped_cluster'
    end as unallocated_reason

from weighted
