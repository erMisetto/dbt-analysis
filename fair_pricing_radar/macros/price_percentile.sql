{% macro price_percentile(col, p) %}
    percentile_cont({{ p }}) within group (order by {{ col }}) over()
{% endmacro %}