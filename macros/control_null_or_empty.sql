{% macro control_null_or_empty(column_name, default_value) %}
    CASE
        WHEN {{ column_name }} IS NULL OR {{ column_name }} = '' 
            THEN {{ dbt_utils.generate_surrogate_key([default_value]) }}
        ELSE {{ dbt_utils.generate_surrogate_key([column_name]) }}
    END
{% endmacro %}

--macro que genera claves subrrogadas a un campo
--controla cuando un registro es nulo o '' y devuelve el valor que le especifiques para ese caso