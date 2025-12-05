{% macro airport_region(state_column) %}
    CASE
        WHEN {{ state_column }} IN ('CA','OR','WA') THEN 'WEST'
        WHEN {{ state_column }} IN ('NY','PA','MA','NJ') THEN 'NORTHEAST'
        WHEN {{ state_column }} IN ('TX','FL','GA','AL') THEN 'SOUTH'
        WHEN {{ state_column }} IN ('IL','OH','MI','WI') THEN 'MIDWEST'
        ELSE 'INTERNATIONAL'
    END
{% endmacro %}

{% macro airport_continent(country_column) %}
    CASE
        WHEN {{ country_column }} = 'USA' THEN 'NORTH_AMERICA'
        WHEN {{ country_column }} IN ('CAN','MEX') THEN 'NORTH_AMERICA'
        WHEN {{ country_column }} IN ('IND','CHN','JPN','SGP') THEN 'ASIA'
        WHEN {{ country_column }} IN ('FRA','DEU','ESP','ITA','NLD') THEN 'EUROPE'
        ELSE 'OTHER'
    END
{% endmacro %}

{% macro airport_size(state_column, country_column) %}
    CASE
        WHEN {{ state_column }} = 'CA' THEN 'LARGE'
        WHEN {{ state_column }} IN ('NY','TX','FL','IL') THEN 'MEDIUM'
        WHEN {{ country_column }} != 'USA' THEN 'INTERNATIONAL'
        ELSE 'REGIONAL'
    END
{% endmacro %}