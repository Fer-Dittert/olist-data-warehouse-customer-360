{{ config(materialized='view') }}

with vendas as (

    select *
    from {{ ref('stg_vendas') }}

),

base as (

    select
        v.id_pedido,
        count(*) as quantidade_itens,
        count(distinct v.id_produto) as quantidade_produtos_distintos,
        count(distinct v.id_vendedor) as quantidade_vendedores_distintos,
        sum(v.valor_item) as valor_itens_total,
        sum(v.valor_frete) as valor_frete_total,
        sum(v.valor_item + v.valor_frete) as valor_total_pedido
    from vendas v
    group by v.id_pedido

)

select *
from base