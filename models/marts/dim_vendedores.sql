{{ config(materialized='table') }}

with vendedores as (

    select *
    from {{ ref('stg_vendedores') }}

),

localizacao as (

    select *
    from {{ ref('dim_localizacao') }}
    where origem_localizacao = 'vendedor'

),

final as (

    select distinct
        v.id_vendedor as vendedor_key,
        v.id_vendedor,
        l.id_localizacao
    from vendedores v
    left join localizacao l
        on v.cep_vendedor = l.cep
       and v.cidade_vendedor = l.cidade
       and v.estado_vendedor = l.estado

)

select *
from final