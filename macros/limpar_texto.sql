{% macro limpar_texto(coluna) %}
    lower(trim({{ coluna }}))
{% endmacro %}