{{ config(materialized='table') }}

with source as (

    select distinct
        status_pedido
    from {{ ref('int_vendas_enriquecidas') }}
    where status_pedido is not null

)

select
    row_number() over (order by status_pedido) as id_status_pedido,
    status_pedido
from source