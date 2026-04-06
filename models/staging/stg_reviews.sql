{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('raw_olist', 'order_reviews') }}

),

renamed as (

    select
        review_id as id_avaliacao,
        order_id as id_pedido,
        cast(review_score as integer) as nota_avaliacao,
        trim(review_comment_title) as titulo_comentario,
        trim(review_comment_message) as comentario_avaliacao,
        cast(review_creation_date as timestamp) as data_criacao_avaliacao,
        cast(review_answer_timestamp as timestamp) as data_resposta_avaliacao
    from source

)

select * from renamed