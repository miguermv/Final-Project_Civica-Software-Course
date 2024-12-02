{% test validate_date_order(model, column_name, start_date_column ) %}

select
    *
from
    {{ model }}
where
    {{ start_date_column }} > {{ column_name }}

{% endtest %}