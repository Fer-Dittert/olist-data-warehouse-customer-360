{{ config(materialized='table') }}

with pedidos as (

    select *
    from {{ ref('stg_pedidos') }}

),

clientes as (

    select *
    from {{ ref('stg_clientes') }}

),

itens_pedido as (

    select *
    from {{ ref('int_itens_pedido') }}

),

pagamentos as (

    select *
    from {{ ref('int_pagamentos_pedido') }}

),

reviews as (

    select *
    from {{ ref('int_reviews_pedido') }}

),

base as (

    select
        p.id_pedido,
        p.id_cliente,
        c.id_cliente_unico,
        p.status_pedido,
        p.data_compra,
        p.data_aprovacao,
        p.data_envio_transportadora,
        p.data_entrega_cliente,
        p.data_estimada_entrega,

        c.cep_cliente,
        c.cidade_cliente,
        c.estado_cliente,

        ip.quantidade_itens,
        ip.quantidade_produtos_distintos,
        ip.quantidade_vendedores_distintos,
        ip.valor_itens_total,
        ip.valor_frete_total,
        ip.valor_total_pedido,

        pg.valor_pagamento_total,
        pg.quantidade_parcelas_max,
        pg.quantidade_registros_pagamento,
        pg.quantidade_tipos_pagamento,
        pg.tipo_pagamento_principal,

        rv.nota_avaliacao,
        rv.titulo_comentario,
        rv.comentario_avaliacao,
        rv.data_criacao_avaliacao,
        rv.data_resposta_avaliacao,
        rv.indicador_possui_comentario,
        rv.indicador_review_negativa

    from pedidos p
    left join clientes c
        on p.id_cliente = c.id_cliente
    left join itens_pedido ip
        on p.id_pedido = ip.id_pedido
    left join pagamentos pg
        on p.id_pedido = pg.id_pedido
    left join reviews rv
        on p.id_pedido = rv.id_pedido

),

regras_negocio as (

    select
        *,
        datediff(day, data_compra, data_aprovacao) as dias_aprovacao,
        datediff(day, data_compra, data_entrega_cliente) as dias_entrega,
        datediff(day, data_estimada_entrega, data_entrega_cliente) as dias_atraso_entrega,

        case
            when data_entrega_cliente is not null
             and data_estimada_entrega is not null
             and data_entrega_cliente > data_estimada_entrega then 1
            else 0
        end as indicador_atraso_entrega,

        case
            when status_pedido = 'delivered' then 1
            else 0
        end as indicador_pedido_entregue

    from base

)

select *
from regras_negocio