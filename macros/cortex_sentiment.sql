{% macro cortex_sentiment(column_name) %}
    CASE
        WHEN SNOWFLAKE.CORTEX.SENTIMENT({{ column_name }}) > 0.5 THEN 'Positive'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT({{ column_name }}) BETWEEN -0.5 AND 0.5 THEN 'Neutral'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT({{ column_name }}) < -0.5 THEN 'Negative'
    END
{% endmacro %}

--Lee un texto y decide que tipo de tono tiene ese texto.