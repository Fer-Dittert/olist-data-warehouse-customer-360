{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('raw_olist', 'order_items') }}

),

renamed as (

    select
        order_id as id_pedido,
        cast(order_item_id as integer) as id_item_pedido,
        product_id as id_produto,
        seller_id as id_vendedor,
        cast(shipping_limit_date as timestamp) as data_limite_envio,
        cast(price as numeric(12,2)) as valor_item,
        cast(freight_value as numeric(12,2)) as valor_frete
    from source

),

deduplicado as (

    select
        *,
        row_number() over (
            partition by id_pedido, id_item_pedido
            order by id_produto, id_vendedor
        ) as rn
    from renamed

)

select
    id_pedido,
    id_item_pedido,
    id_produto,
    id_vendedor,
    data_limite_envio,
    valor_item,
    valor_frete
from deduplicado
where rn = 1