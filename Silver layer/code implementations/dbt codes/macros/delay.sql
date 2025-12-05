{% macro delay_threshold() %}
    -- default delay threshold (minutes)
    {{ var('delay_threshold_minutes', 15) }}
{% endmacro %}

{% macro delay_category(total_delay) %}
    case
        when {{ total_delay }} <= 0 then 'NO_DELAY'
        when {{ total_delay }} <= {{ delay_threshold() }} then 'SHORT'
        when {{ total_delay }} <= 60 then 'MEDIUM'
        else 'LONG'
    end
{% endmacro %}