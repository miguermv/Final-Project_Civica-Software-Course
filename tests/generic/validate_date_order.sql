{% test validate_date_order(model, start_date_column, end_date_column) %}

select
    *
from
    {{ model }}
where
    {{ start_date_column }} > {{ end_date_column }}

{% endtest %}