
{% macro test_relationships(model, field, to, from) %}


select count(*)
from (

    select
        {{ from }} as id

    from {{ model }}
    where {{ from }} is not null
      and {{ from }} not in (select {{ field }}
                             from {{ to }})

) validation_errors

{% endmacro %}

{% macro test_foreign_key(model, column_name, field, to) %}


select count(*)
from (

    select
        {{ column_name }} as id

    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} not in (select {{ field }}
                             from {{ to }})

) validation_errors

{% endmacro %}
