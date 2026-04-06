{{ config(
    materialized='incremental',
    unique_key=['id_pedido', 'id_item_pedido'],
    dist='id_pedido',
    sort='id_data'
) }}

with vendas as (

    select *
    from {{ ref('int_vendas_enriquecidas') }}

),

dim_status as (

    select
        id_status_pedido,
        status_pedido
    from {{ ref('dim_status_pedido') }}

),

final as (

    select
        v.id_pedido,
        v.id_item_pedido,
        v.id_cliente,
        v.id_produto,
        v.id_vendedor,
        cast(v.data_compra as date) as id_data,
        ds.id_status_pedido,
        v.valor_item,
        v.valor_frete,
        v.valor_total_item,
        v.indicador_entregue,
        v.indicador_atraso,
        v.dias_entrega

    from vendas v
    left join dim_status ds
        on v.status_pedido = ds.status_pedido

    where v.id_cliente is not null
      and v.data_compra is not null
      and ds.id_status_pedido is not null

)

select *
from final

{% if is_incremental() %}
where id_data > (
    select coalesce(max(id_data), '1900-01-01'::date)
    from {{ this }}
)
{% endif %}