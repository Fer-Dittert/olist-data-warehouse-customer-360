{{ config(materialized='view') }}

with reviews as (

    select *
    from {{ ref('stg_reviews') }}

),

base as (

    select
        id_pedido,
        max(nota_avaliacao) as nota_avaliacao,
        max(titulo_comentario) as titulo_comentario,
        max(comentario_avaliacao) as comentario_avaliacao,
        max(data_criacao_avaliacao) as data_criacao_avaliacao,
        max(data_resposta_avaliacao) as data_resposta_avaliacao,
        case
            when max(comentario_avaliacao) is not null then 1
            else 0
        end as indicador_possui_comentario,
        case
            when max(nota_avaliacao) <= 2 then 1
            else 0
        end as indicador_review_negativa
    from reviews
    group by id_pedido

)

select *
from base