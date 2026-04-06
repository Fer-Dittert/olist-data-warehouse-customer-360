{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('raw_olist', 'products') }}

),

renamed as (

    select
        product_id as id_produto,
        trim(product_category_name) as categoria_produto,
        cast(product_name_lenght as integer) as tamanho_nome_produto,
        cast(product_description_lenght as integer) as tamanho_descricao_produto,
        cast(product_photos_qty as integer) as quantidade_fotos_produto,
        cast(product_weight_g as integer) as peso_produto_g,
        cast(product_length_cm as integer) as comprimento_produto_cm,
        cast(product_height_cm as integer) as altura_produto_cm,
        cast(product_width_cm as integer) as largura_produto_cm
    from source

)

select * from renamed