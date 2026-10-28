-- Billing crudo tipado 1:1 con la fuente. La corrección de la cuenta 846
-- (seed cost_corrections) se aplica en int_billing_lines_corrected: staging
-- no junta fuentes ni aplica reglas de negocio.

with source as (

    select * from {{ source('raw_dss', 'cluster_billing_lines') }}

),

renamed as (

    select
        line_id,
        billing_period,
        usage_date,
        provider,
        account_id,
        product_code,
        usage_type,
        nullif(trim(tag_cluster), '') as tag_cluster,
        cost,
        nullif(trim(tag_cluster), '') is null as is_untagged

    from source

)

select * from renamed
