
{% macro test_primary_key(model, arg) %}

select count(*)
from (

    select
        {{ arg }}

    from {{ model }}
    group by {{ arg }}
    having count(*) > 1

) validation_errors

{% endmacro %}
