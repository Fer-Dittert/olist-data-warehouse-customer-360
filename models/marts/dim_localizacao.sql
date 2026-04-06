{{ config(materialized='table') }}

with clientes_localizacao as (

    select distinct
        cep_cliente as cep,
        cidade_cliente as cidade,
        estado_cliente as estado,
        'cliente' as origem_localizacao
    from {{ ref('stg_clientes') }}

),

vendedores_localizacao as (

    select distinct
        cep_vendedor as cep,
        cidade_vendedor as cidade,
        estado_vendedor as estado,
        'vendedor' as origem_localizacao
    from {{ ref('stg_vendedores') }}

),

unioned as (

    select * from clientes_localizacao
    union
    select * from vendedores_localizacao

),

final as (

    select
        row_number() over (order by estado, cidade, cep, origem_localizacao) as id_localizacao,
        cep,
        cidade,
        estado,
        origem_localizacao
    from unioned

)

select *
from final