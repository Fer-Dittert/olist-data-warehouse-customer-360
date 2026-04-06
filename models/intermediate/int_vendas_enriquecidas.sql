{{ config(materialized='view') }}

with vendas as (

    select *
    from {{ ref('stg_vendas') }}

),

pedidos as (

    select *
    from {{ ref('stg_pedidos') }}

),

clientes as (

    select *
    from {{ ref('stg_clientes') }}

),

produtos as (

    select *
    from {{ ref('stg_produtos') }}

),

vendedores as (

    select *
    from {{ ref('stg_vendedores') }}

),

pagamentos as (

    select *
    from {{ ref('stg_pagamentos') }}

),

avaliacoes as (

    select *
    from {{ ref('stg_reviews') }}

),

pagamentos_agregados as (

    select
        id_pedido,
        sum(valor_pagamento) as valor_pagamento_total,
        max(quantidade_parcelas) as quantidade_parcelas,
        max(tipo_pagamento) as tipo_pagamento
    from pagamentos
    group by id_pedido

),

avaliacoes_agregadas as (

    select
        id_pedido,
        max(nota_avaliacao) as nota_avaliacao,
        max(titulo_comentario) as titulo_comentario,
        max(comentario_avaliacao) as comentario_avaliacao,
        max(data_criacao_avaliacao) as data_criacao_avaliacao,
        max(data_resposta_avaliacao) as data_resposta_avaliacao
    from avaliacoes
    group by id_pedido

),

joined as (

    select
        v.id_pedido,
        v.id_item_pedido,
        p.id_cliente,
        v.id_produto,
        v.id_vendedor,

        p.status_pedido,
        p.data_compra,
        p.data_aprovacao,
        p.data_envio_transportadora,
        p.data_entrega_cliente,
        p.data_estimada_entrega,

        v.data_limite_envio,
        v.valor_item,
        v.valor_frete,
        (v.valor_item + v.valor_frete) as valor_total_item,

        c.id_cliente_unico,
        c.cep_cliente,
        c.cidade_cliente,
        c.estado_cliente,

        pr.categoria_produto,
        pr.tamanho_nome_produto,
        pr.tamanho_descricao_produto,
        pr.quantidade_fotos_produto,
        pr.peso_produto_g,
        pr.comprimento_produto_cm,
        pr.altura_produto_cm,
        pr.largura_produto_cm,

        vd.cep_vendedor,
        vd.cidade_vendedor,
        vd.estado_vendedor,

        pg.valor_pagamento_total,
        pg.quantidade_parcelas,
        pg.tipo_pagamento,

        av.nota_avaliacao,
        av.titulo_comentario,
        av.comentario_avaliacao,
        av.data_criacao_avaliacao,
        av.data_resposta_avaliacao

    from vendas v
    left join pedidos p
        on v.id_pedido = p.id_pedido
    left join clientes c
        on p.id_cliente = c.id_cliente
    left join produtos pr
        on v.id_produto = pr.id_produto
    left join vendedores vd
        on v.id_vendedor = vd.id_vendedor
    left join pagamentos_agregados pg
        on v.id_pedido = pg.id_pedido
    left join avaliacoes_agregadas av
        on v.id_pedido = av.id_pedido

),

business_rules as (

    select
        *,
        case 
            when status_pedido = 'delivered' then 1
            else 0
        end as indicador_entregue,

        case
            when data_entrega_cliente is not null
             and data_estimada_entrega is not null
             and data_entrega_cliente > data_estimada_entrega then 1
            else 0
        end as indicador_atraso,

        case
            when valor_item < 50 then 'baixo'
            when valor_item between 50 and 200 then 'medio'
            else 'alto'
        end as faixa_valor_item,

        datediff(day, data_compra, data_entrega_cliente) as dias_entrega

    from joined

)

select *
from business_rules