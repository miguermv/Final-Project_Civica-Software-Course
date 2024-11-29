{% macro ifvalue_to_bool(column_name, value) %}
    CASE
        WHEN {{ column_name }} = {{ value }} 
            THEN TRUE
        ELSE FALSE
    END
{% endmacro %}

--macro que transforma en bool un campo
--controla cuando un registro es de un valor especifico y marca todos como TRUE, al resto los deja como FALSE