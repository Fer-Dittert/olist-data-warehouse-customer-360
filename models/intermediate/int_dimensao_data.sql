{{ config(materialized='view') }}

with date_spine as (

    {{
        dbt_utils.date_spine(
            datepart = "day",
            start_date = "cast('2016-01-01' as date)",
            end_date = "cast('2019-01-01' as date)"
        )
    }}

),

criar_datas as (

    select
        cast(date_day as date) as id_data,
        cast(date_day as date) as data_completa,
        extract(day from date_day) as dia,
        extract(month from date_day) as mes,
        extract(year from date_day) as ano,
        extract(quarter from date_day) as trimestre,
        extract(dow from date_day) as dia_semana_numero,
        trim(to_char(cast(date_day as date), 'Month')) as nome_mes,
        trim(to_char(cast(date_day as date), 'Day')) as nome_dia_semana,
        case
            when extract(dow from date_day) in (0, 6) then 1
            else 0
        end as fim_de_semana
    from date_spine

)

select *
from criar_datas