{{ config(materialized='table') }}

with clientes as (

    select *
    from {{ ref('stg_clientes') }}

),

localizacao as (

    select *
    from {{ ref('dim_localizacao') }}
    where origem_localizacao = 'cliente'

),

final as (

    select distinct
        c.id_cliente as cliente_key,
        c.id_cliente,
        c.id_cliente_unico,
        l.id_localizacao
    from clientes c
    left join localizacao l
        on c.cep_cliente = l.cep
       and c.cidade_cliente = l.cidade
       and c.estado_cliente = l.estado

)

select *
from final