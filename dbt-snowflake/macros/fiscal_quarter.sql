{% macro fiscal_quarter(date_col) %}
    concat(
        cast(extract(year from {{ date_col }}) as varchar),
        '-Q',
        cast(extract(quarter from {{ date_col }}) as varchar)
    )
{% endmacro %}
