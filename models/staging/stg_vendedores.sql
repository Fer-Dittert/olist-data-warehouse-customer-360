{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('raw_olist', 'sellers') }}

),

renamed as (

    select
        seller_id as id_vendedor,
        cast(seller_zip_code_prefix as integer) as cep_vendedor,
        trim(seller_city) as cidade_vendedor,
        trim(seller_state) as estado_vendedor
    from source

)

select * from renamed