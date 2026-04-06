{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('raw_olist', 'customers') }}

),

renamed as (

    select
        customer_id as id_cliente,
        customer_unique_id as id_cliente_unico,
        cast(customer_zip_code_prefix as integer) as cep_cliente,
        {{ limpar_texto('customer_city') }} as cidade_cliente,
        upper(trim(customer_state)) as estado_cliente
    from source

)

select * from renamed