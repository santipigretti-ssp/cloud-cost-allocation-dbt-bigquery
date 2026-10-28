-- Billing con correcciones de la seed cost_corrections aplicadas (caso cuenta
-- 846, ~x1000 fuera de escala). Todo lo downstream lee costo desde acá, nunca
-- desde staging: cost es el corregido, cost_original queda para auditoría.

with billing as (

    select * from {{ ref('stg_billing_lines') }}

),

corrections as (

    select * from {{ ref('cost_corrections') }}

),

corrected as (

    select
        billing.line_id,
        billing.billing_period,
        billing.usage_date,
        billing.provider,
        billing.account_id,
        billing.product_code,
        billing.usage_type,
        billing.tag_cluster,
        billing.is_untagged,
        billing.cost as cost_original,
        coalesce(corrections.correction_factor, 1.0) as correction_factor,
        billing.cost * coalesce(corrections.correction_factor, 1.0) as cost,
        corrections.correction_factor is not null as is_corrected

    from billing
    left join corrections
        on billing.account_id = corrections.account_id
        -- vigencia semiabierta [valid_from, valid_to): misma convención que
        -- int_entity_set_daily. Evita que el día del borde caiga en dos
        -- correcciones consecutivas de una cuenta (doble factor).
        and billing.usage_date >= corrections.valid_from
        and billing.usage_date < corrections.valid_to

)

select * from corrected
