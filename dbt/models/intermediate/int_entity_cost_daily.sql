-- Costo del set repartido a sus entidades vigentes, proporcional al uso del
-- driver. Salida única para asignado y no asignado (los marts filtran):
--   · set_usage_total > 0  → prorrata, allocation_method = 'usage_proportional'
--   · uso 0 y 1 sola entidad vigente → 100%, 'sole_member_no_usage'
--     (caso cuenta 512 / septiembre: app-pricing-04)
--   · uso 0 y varias entidades → even-split entre ellas, 'even_split_no_usage'.
--     Decisión: la capacidad reservada ociosa de un pool se reparte en partes
--     iguales entre sus entidades vigentes, igual espíritu que sole_member (que
--     es el even-split de N=1) y que el fallback de clusters compartidos. Evita
--     la discontinuidad indefendible N=1 (cobra 100%) vs N>1 (no cobraba nada).
--     Flageado, nunca silencioso (assert_no_usage_allocations_flagged).
--   · set sin entidades vigentes (cs-010/035/040) → 'orphan_cluster_set'
--   · clusters sin set → pasan directo con 'unmapped_cluster'

with set_cost as (

    select
        billing_period,
        usage_date,
        cluster_set_id,
        cost_driver,
        sum(cost) as cost,
        logical_or(is_corrected_source) as is_corrected_source
    from {{ ref('int_cluster_set_cost_daily') }}
    where unallocated_reason is null
    group by billing_period, usage_date, cluster_set_id, cost_driver

),

entity_sets as (

    select * from {{ ref('int_entity_set_daily') }}

),

usage as (

    select * from {{ ref('int_usage_daily') }}

),

-- entidades vigentes del set ese día, con su uso del driver (0 si no reportó)
member_usage as (

    select
        set_cost.billing_period,
        set_cost.usage_date,
        set_cost.cluster_set_id,
        set_cost.cost_driver,
        set_cost.cost,
        set_cost.is_corrected_source,
        entity_sets.entity_id,
        coalesce(usage.usage_amount, 0) as entity_usage

    from set_cost
    inner join entity_sets
        on entity_sets.cluster_set_id = set_cost.cluster_set_id
        and entity_sets.usage_date = set_cost.usage_date
    left join usage
        on usage.entity_id = entity_sets.entity_id
        and usage.usage_date = set_cost.usage_date
        and usage.cluster_set_id = set_cost.cluster_set_id
        and usage.cost_driver = set_cost.cost_driver

),

set_stats as (

    select
        usage_date,
        cluster_set_id,
        cost_driver,
        count(*) as members_count,
        sum(entity_usage) as set_usage_total
    from member_usage
    group by usage_date, cluster_set_id, cost_driver

),

allocated as (

    select
        member_usage.billing_period,
        member_usage.usage_date,
        member_usage.entity_id,
        member_usage.cluster_set_id,
        cast(null as string) as cluster_id,
        member_usage.cost_driver,
        case
            when set_stats.set_usage_total > 0
                then member_usage.cost * member_usage.entity_usage / set_stats.set_usage_total
            -- sin uso: even-split (N=1 → cost/1 = 100%, el caso sole_member)
            else member_usage.cost / set_stats.members_count
        end as cost,
        case
            when set_stats.set_usage_total > 0 then 'usage_proportional'
            when set_stats.members_count = 1 then 'sole_member_no_usage'
            else 'even_split_no_usage'
        end as allocation_method,
        cast(null as string) as unallocated_reason,
        member_usage.is_corrected_source

    from member_usage
    inner join set_stats
        using (usage_date, cluster_set_id, cost_driver)

    -- todo set con al menos una entidad vigente se asigna; el único no asignado
    -- de esta rama es el set sin entidades (orphan_cluster_set, abajo)

),

unallocated_sets as (

    select
        set_cost.billing_period,
        set_cost.usage_date,
        cast(null as string) as entity_id,
        set_cost.cluster_set_id,
        cast(null as string) as cluster_id,
        set_cost.cost_driver,
        set_cost.cost,
        cast(null as string) as allocation_method,
        'orphan_cluster_set' as unallocated_reason,
        set_cost.is_corrected_source

    from set_cost
    left join set_stats
        using (usage_date, cluster_set_id, cost_driver)

    where set_stats.members_count is null

),

unmapped_clusters as (

    select
        billing_period,
        usage_date,
        cast(null as string) as entity_id,
        cluster_set_id,
        cluster_id,
        cost_driver,
        cost,
        cast(null as string) as allocation_method,
        unallocated_reason,
        is_corrected_source

    from {{ ref('int_cluster_set_cost_daily') }}
    where unallocated_reason = 'unmapped_cluster'

)

select * from allocated
union all
select * from unallocated_sets
union all
select * from unmapped_clusters
