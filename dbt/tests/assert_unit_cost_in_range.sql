-- Detector de futuros "casos 846": el costo unitario implícito por
-- cuenta/driver (costo corregido / uso de los sets que factura la cuenta)
-- debe estar dentro de [0.1x, 10x] de la mediana global del driver. Si una
-- cuenta vuelve a llegar fuera de escala, este test la señala antes de que
-- contamine la asignación.

with account_sets as (

    select distinct
        billing.account_id,
        membership.cluster_set_id
    from {{ ref('int_billing_lines_corrected') }} as billing
    inner join {{ ref('stg_cluster_set_membership') }} as membership
        on billing.tag_cluster = membership.cluster_id

),

account_usage as (

    select
        account_sets.account_id,
        usage.cost_driver,
        sum(usage.usage_amount) as usage_amount
    from account_sets
    inner join {{ ref('int_usage_daily') }} as usage
        on usage.cluster_set_id = account_sets.cluster_set_id
    group by account_sets.account_id, usage.cost_driver

),

account_cost as (

    select
        account_id,
        cost_driver,
        sum(cost) as cost
    from {{ ref('int_cluster_cost_daily') }}
    where cost_driver in ('compute', 'storage')
    group by account_id, cost_driver

),

unit_costs as (

    select
        account_cost.account_id,
        account_cost.cost_driver,
        account_cost.cost / nullif(account_usage.usage_amount, 0) as unit_cost
    from account_cost
    inner join account_usage
        using (account_id, cost_driver)

),

driver_median as (

    select
        cost_driver,
        approx_quantiles(unit_cost, 100)[offset(50)] as median_unit_cost
    from unit_costs
    where unit_cost is not null
    group by cost_driver

)

select
    unit_costs.account_id,
    unit_costs.cost_driver,
    unit_costs.unit_cost,
    driver_median.median_unit_cost

from unit_costs
inner join driver_median
    using (cost_driver)

where unit_costs.unit_cost is not null
    and (
        unit_costs.unit_cost < 0.1 * driver_median.median_unit_cost
        or unit_costs.unit_cost > 10 * driver_median.median_unit_cost
    )
