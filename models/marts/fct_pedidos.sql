{{ config(
    materialized='incremental',
    unique_key='id_pedido',
    dist='id_cliente',
    sort='id_data_compra'
) }}

with pedidos as (

    select *
    from {{ ref('int_pedidos_enriquecidos') }}

),

dim_status as (

    select *
    from {{ ref('dim_status_pedido') }}

),

final as (

    select
        p.id_pedido,
        p.id_cliente,
        cast(p.data_compra as date) as id_data_compra,
        cast(p.data_entrega_cliente as date) as id_data_entrega,
        ds.id_status_pedido,

        p.status_pedido,
        p.quantidade_itens,
        p.quantidade_produtos_distintos,
        p.quantidade_vendedores_distintos,

        p.valor_itens_total,
        p.valor_frete_total,
        p.valor_total_pedido,
        p.valor_pagamento_total,

        p.quantidade_parcelas_max,
        p.quantidade_registros_pagamento,
        p.quantidade_tipos_pagamento,
        p.tipo_pagamento_principal,

        p.nota_avaliacao,
        p.indicador_possui_comentario,
        p.indicador_review_negativa,

        p.dias_aprovacao,
        p.dias_entrega,
        p.dias_atraso_entrega,
        p.indicador_atraso_entrega,
        p.indicador_pedido_entregue

    from pedidos p
    left join dim_status ds
        on p.status_pedido = ds.status_pedido

    where p.id_cliente is not null
      and p.data_compra is not null

)

select *
from final

{% if is_incremental() %}
where id_data_compra > (
    select coalesce(max(id_data_compra), '1900-01-01'::date)
    from {{ this }}
)
{% endif %}