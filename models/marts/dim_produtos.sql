{{ config(materialized='table') }}

select distinct
    id_produto as produto_key,
    id_produto,
    categoria_produto,
    tamanho_nome_produto,
    tamanho_descricao_produto,
    quantidade_fotos_produto,
    peso_produto_g,
    comprimento_produto_cm,
    altura_produto_cm,
    largura_produto_cm
from {{ ref('stg_produtos') }}