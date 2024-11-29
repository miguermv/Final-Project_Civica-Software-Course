{% macro cortex_translate(column_name) %}
    SNOWFLAKE.CORTEX.TRANSLATE
    (
        {{ column_name }}, 
        'es', 
        'en'
    )
{% endmacro %}

--macro que traduce con cortex del español al ingles
