{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('raw_olist', 'order_payments') }}

),

renamed as (

    select
        order_id as id_pedido,
        cast(payment_sequential as integer) as sequencial_pagamento,
        trim(payment_type) as tipo_pagamento,
        cast(payment_installments as integer) as quantidade_parcelas,
        cast(payment_value as numeric(12,2)) as valor_pagamento
    from source

)

select * from renamed