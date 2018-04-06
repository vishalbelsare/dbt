{% macro postgres__create_table_as(temporary, relation, sql) -%}
  {%- if temporary -%}
    {%- set relation = relation.include(schema=False) -%}
  {%- endif -%}

  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation }}
  as (
    {{ sql }}
  );
{% endmacro %}
