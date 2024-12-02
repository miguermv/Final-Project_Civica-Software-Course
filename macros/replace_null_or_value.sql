{% macro replace_null_or_value(column_name, to_null, final_value) %}

        COALESCE(NULLIF({{ column_name }}, {{ to_null }}), {{ final_value }})

{% endmacro %}

--Esta macro unifica en un mismo valor (final_value) los casos que sean NULL y los casos que sean un valor especificado (to_null)