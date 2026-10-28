-- Lista anomalías de uso de FILA ÚNICA: uso > umbral × mediana móvil de la
-- entidad/driver, con mediana > 0 y SIN otra fila de patrón para la misma
-- fecha (por eso la regla de outlier NO las excluye — no hay medición de
-- reemplazo ni evidencia de grano roto). NO se excluyen del peso: un pico real
-- debe cobrarse a quien lo generó. Se listan como insumo de revisión con el
-- dueño del servicio: ¿pico real (migración/backfill) o error de unidad?
--
-- Filtrar mediana > 0 evita spamear con los rearranques legítimos de apps que
-- estuvieron en cero (mediana 0 → cualquier valor supera N×0). Limitación
-- honesta: un pico en una entidad sin historia positiva no se puede medir en
-- "×mediana" y no aparece acá.
--
-- severity warn: nunca frena el pipeline (asignado pero nunca silencioso).

{{ config(severity='warn') }}

select
    billing_period,
    usage_date,
    entity_id,
    cost_driver,
    usage_amount,
    rolling_median_usage,
    round(usage_amount / nullif(rolling_median_usage, 0), 1) as x_median

from {{ ref('int_usage_flagged') }}

where usage_amount > {{ var('outlier_threshold_multiplier') }} * rolling_median_usage
    and rolling_median_usage > 0
    and not is_outlier -- is_outlier ya cubre las que tienen fila compañera

order by x_median desc
