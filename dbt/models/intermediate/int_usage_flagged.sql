-- Uso diario de AMBOS drivers (compute + storage) con detección de outliers,
-- regla simétrica por entidad × cost_driver. Nada del modo de falla "fila
-- duplicada con valor corrupto" es propio de storage, así que compute pasa por
-- la misma regla.
--
-- is_outlier = uso > {{ var('outlier_threshold_multiplier') }}× la mediana móvil de
-- {{ var('outlier_window_days') }} días de la entidad/driver Y coexiste otra fila de
-- patrón normal para la misma entidad/fecha/driver. La condición dual es
-- deliberada: excluir del peso solo cuando hay evidencia POSITIVA de registro
-- corrupto (grano roto + medición de reemplazo disponible), nunca ante un valor
-- alto aislado — un pico real debe cobrarse a quien lo generó, no diluirse en
-- las demás entidades. Las anomalías de fila única (sin par) NO se excluyen; las
-- lista assert_single_row_usage_spikes como insumo de revisión.
--
-- Mediana móvil vía self-join por rango de fechas + APPROX_QUANTILES (el
-- PERCENTILE_CONT analítico de BQ no acepta frame de ventana). El driver ya
-- codifica la unidad (compute = vCPU-hours, storage = GB), por eso no se
-- arrastra usage_unit.

with usage as (

    select
        billing_period,
        usage_date,
        entity_id,
        usage_amount,
        'compute' as cost_driver
    from {{ ref('stg_usage_compute') }}

    union all

    select
        billing_period,
        usage_date,
        entity_id,
        usage_amount,
        'storage' as cost_driver
    from {{ ref('stg_usage_storage') }}

),

with_rolling_median as (

    select
        current_row.billing_period,
        current_row.usage_date,
        current_row.entity_id,
        current_row.cost_driver,
        current_row.usage_amount,
        approx_quantiles(history.usage_amount, 100)[offset(50)] as rolling_median_usage

    from usage as current_row
    inner join usage as history
        on history.entity_id = current_row.entity_id
        and history.cost_driver = current_row.cost_driver
        and history.usage_date
            between date_sub(current_row.usage_date, interval {{ var('outlier_window_days') }} day)
            and current_row.usage_date

    group by
        current_row.billing_period,
        current_row.usage_date,
        current_row.entity_id,
        current_row.cost_driver,
        current_row.usage_amount

),

flagged as (

    select
        billing_period,
        usage_date,
        entity_id,
        cost_driver,
        usage_amount,
        rolling_median_usage,
        usage_amount > {{ var('outlier_threshold_multiplier') }} * rolling_median_usage
            and countif(usage_amount <= {{ var('outlier_threshold_multiplier') }} * rolling_median_usage)
                over (partition by entity_id, usage_date, cost_driver) >= 1
            as is_outlier

    from with_rolling_median

)

select * from flagged
