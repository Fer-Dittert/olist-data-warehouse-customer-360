{{ config(materialized='table') }}

select *
from {{ ref('int_dimensao_data') }}