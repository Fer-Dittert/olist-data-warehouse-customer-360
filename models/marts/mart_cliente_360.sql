{{ config(materialized='table') }}

with clientes as (

    select *
    from {{ ref('dim_clientes') }}

),

metricas as (

    select *
    from {{ ref('fct_cliente_metricas') }}

),

localizacao as (

    select *
    from {{ ref('dim_localizacao') }}
    where origem_localizacao = 'cliente'

),

final as (

    select
        c.id_cliente,
        c.id_cliente_unico,
        l.cep,
        l.cidade,
        l.estado,

        m.primeira_compra,
        m.ultima_compra,
        m.total_pedidos,
        m.valor_total_gasto,
        m.ticket_medio,
        m.media_itens_por_pedido,
        m.media_dias_entrega,
        m.media_nota_avaliacao,
        m.total_pedidos_atrasados,
        m.total_reviews_negativas,
        m.maximo_parcelas_utilizadas,
        m.dias_desde_ultima_compra,
        m.segmento_frequencia,
        m.segmento_valor,
        m.segmento_satisfacao,
        m.indicador_risco_churn

    from clientes c
    left join metricas m
        on c.id_cliente = m.id_cliente
    left join localizacao l
        on c.id_localizacao = l.id_localizacao

)

select *
from final