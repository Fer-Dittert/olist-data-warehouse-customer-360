{{ config(materialized='view') }}

with pagamentos as (

    select *
    from {{ ref('stg_pagamentos') }}

),

pagamentos_resumidos as (

    select
        id_pedido,
        tipo_pagamento,
        count(*) as quantidade_ocorrencias_tipo,
        sum(valor_pagamento) as valor_total_tipo
    from pagamentos
    group by
        id_pedido,
        tipo_pagamento

),

pagamento_principal as (

    select
        id_pedido,
        tipo_pagamento,
        quantidade_ocorrencias_tipo,
        valor_total_tipo,
        row_number() over (
            partition by id_pedido
            order by quantidade_ocorrencias_tipo desc, valor_total_tipo desc, tipo_pagamento asc
        ) as rn
    from pagamentos_resumidos

),

pagamentos_agregados as (

    select
        id_pedido,
        sum(valor_pagamento) as valor_pagamento_total,
        max(quantidade_parcelas) as quantidade_parcelas_max,
        count(*) as quantidade_registros_pagamento,
        count(distinct tipo_pagamento) as quantidade_tipos_pagamento
    from pagamentos
    group by id_pedido

),

final as (

    select
        pa.id_pedido,
        pa.valor_pagamento_total,
        pa.quantidade_parcelas_max,
        pa.quantidade_registros_pagamento,
        pa.quantidade_tipos_pagamento,
        pp.tipo_pagamento as tipo_pagamento_principal
    from pagamentos_agregados pa
    left join pagamento_principal pp
        on pa.id_pedido = pp.id_pedido
       and pp.rn = 1

)

select *
from final