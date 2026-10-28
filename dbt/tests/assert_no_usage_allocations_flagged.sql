-- Asignado pero nunca silencioso: lista todo costo asignado SIN señal de uso —
-- capacidad reservada ociosa repartida por regla, no por uso medido:
--   · sole_member_no_usage — pool de 1 entidad, 100% a ella (caso 512 /
--     septiembre / app-pricing-04; también app-buyer-07 y app-payments-05 abril)
--   · even_split_no_usage  — pool de N>1 entidades ociosas, repartido en partes
--     iguales (hoy no ocurre; queda armado para datos nuevos)
-- Ambos son decisiones de chargeback que el dueño debe poder auditar y
-- eventualmente vetar. severity warn: alerta en cada corrida sin frenar.

{{ config(severity='warn') }}

select
    billing_period,
    entity_id,
    cost_driver,
    allocation_method,
    cost

from {{ ref('fct_cost_allocated_entity_period') }}
where allocation_method in ('sole_member_no_usage', 'even_split_no_usage')
order by allocation_method, billing_period, entity_id, cost_driver
