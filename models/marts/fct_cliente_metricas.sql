{{ config(
    materialized='table',
    dist='id_cliente',
    sort='id_cliente'
) }}

with pedidos as (

    select *
    from {{ ref('fct_pedidos') }}

),

base as (

    select
        id_cliente,
        min(id_data_compra) as primeira_compra,
        max(id_data_compra) as ultima_compra,
        count(distinct id_pedido) as total_pedidos,
        sum(valor_total_pedido) as valor_total_gasto,
        avg(valor_total_pedido) as ticket_medio,
        avg(quantidade_itens) as media_itens_por_pedido,
        avg(dias_entrega) as media_dias_entrega,
        avg(nota_avaliacao) as media_nota_avaliacao,
        sum(indicador_atraso_entrega) as total_pedidos_atrasados,
        sum(indicador_review_negativa) as total_reviews_negativas,
        max(quantidade_parcelas_max) as maximo_parcelas_utilizadas
    from pedidos
    group by id_cliente

),

segmentado as (

    select
        *,
        datediff(day, ultima_compra, current_date) as dias_desde_ultima_compra,

        case
            when total_pedidos = 1 then 'cliente_unico'
            when total_pedidos between 2 and 4 then 'recorrente'
            else 'frequente'
        end as segmento_frequencia,

        case
            when valor_total_gasto < 200 then 'baixo_valor'
            when valor_total_gasto between 200 and 1000 then 'medio_valor'
            else 'alto_valor'
        end as segmento_valor,

        case
            when media_nota_avaliacao >= 4 then 'alta_satisfacao'
            when media_nota_avaliacao >= 3 then 'satisfacao_media'
            else 'baixa_satisfacao'
        end as segmento_satisfacao,

        case
            when datediff(day, ultima_compra, current_date) > 180 then 1
            else 0
        end as indicador_risco_churn

    from base

)

select *
from segmentado