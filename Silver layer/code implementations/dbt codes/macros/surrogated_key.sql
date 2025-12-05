{% macro generate_sk(columns) %}
    -- columns should be a list of column names
    md5(
        {{ columns | map('lower') | join(" || '|' || ") }}
    )
{% endmacro %}
