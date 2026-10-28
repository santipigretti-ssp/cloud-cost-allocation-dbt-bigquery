-- Guarda el supuesto sobre el que se apoya todo el modelo de overhead: hoy las
-- dos dimensiones ortogonales del billing coinciden — todo EC2/S3 viene
-- tageado (atribuible a un cluster) y todo servicio de otra clase
-- (DataTransfer/CloudWatch/GuardDuty/Support) viene sin tag (overhead).
--
-- Mientras eso se cumple, `where is_untagged` (que dispara el prorrateo de
-- overhead en el mart) y el mapeo product_code→cost_driver dan lo mismo. Si un
-- feed futuro rompe la alineación, el ruteo se vuelve silencioso e
-- inconsistente:
--   · EC2/S3 SIN tag → se barre a untagged_overhead (costo de compute/storage
--     reportado como overhead genérico, perdiendo el driver).
--   · no-EC2/S3 CON tag → cae en la rama 'overhead' de int_cluster_cost_daily y
--     se enruta por la cascada de uso como driver fantasma, no como overhead.
--
-- Este test falla ante cualquiera de los dos casos: obliga a decidir
-- explícitamente en vez de misroutear. severity error (rompe el build): es un
-- supuesto estructural, no una anomalía tolerable.

with billing as (

    select
        product_code,
        product_code in ('AmazonEC2', 'AmazonS3') as is_compute_or_storage,
        nullif(trim(tag_cluster), '') is null as is_untagged
    from {{ source('raw_dss', 'cluster_billing_lines') }}

)

select
    product_code,
    is_compute_or_storage,
    is_untagged,
    count(*) as offending_rows

from billing
-- las dos violaciones de la alineación: compute/storage sin tag, u overhead con tag
where (is_compute_or_storage and is_untagged)
    or (not is_compute_or_storage and not is_untagged)

group by product_code, is_compute_or_storage, is_untagged
