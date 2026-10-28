-- Costo corregido por (día, cluster, driver, cuenta), excluyendo las líneas
-- sin tag: ese overhead (DataTransfer/CloudWatch/GuardDuty/Support) no se
-- asigna por cluster — se prorratea a nivel mart sobre el costo directo.
-- Mapeo product_code → cost_driver: EC2=compute, S3=storage, resto=overhead
-- (hoy todo lo no-EC2/S3 viene sin tag, la rama overhead es defensiva).

with billing as (

    select * from {{ ref('int_billing_lines_corrected') }}
    where not is_untagged

)

select
    billing_period,
    usage_date,
    account_id,
    tag_cluster as cluster_id,
    case product_code
        when 'AmazonEC2' then 'compute'
        when 'AmazonS3' then 'storage'
        else 'overhead'
    end as cost_driver,
    sum(cost) as cost,
    logical_or(is_corrected) as is_corrected_source

from billing

group by
    billing_period,
    usage_date,
    account_id,
    tag_cluster,
    cost_driver
