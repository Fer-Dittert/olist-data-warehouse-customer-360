{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('raw_olist', 'orders') }}

),

renamed as (

    select
        order_id as id_pedido,
        customer_id as id_cliente,
        trim(order_status) as status_pedido,
        cast(order_purchase_timestamp as timestamp) as data_compra,
        cast(order_approved_at as timestamp) as data_aprovacao,
        cast(order_delivered_carrier_date as timestamp) as data_envio_transportadora,
        cast(order_delivered_customer_date as timestamp) as data_entrega_cliente,
        cast(order_estimated_delivery_date as timestamp) as data_estimada_entrega
    from source

)

select * from renamed