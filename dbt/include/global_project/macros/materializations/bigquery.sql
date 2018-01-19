{% materialization view, adapter='bigquery' -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {%- if existing_type is not none -%}
    {{ adapter.drop(schema, identifier, existing_type) }}
  {%- endif -%}

  -- build model
  {% set result = adapter.execute_model(model, 'view') %}
  {{ store_result('main', status=result) }}

{%- endmaterialization %}

{% materialization table, adapter='bigquery' -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {%- set partition_date = config.get('partition_date') -%}

  {#
      Since dbt uses WRITE_TRUNCATE mode for tables, we only need to drop this thing
      if it is not a table. If it _is_ already a table, then we can overwrite it without downtime
  #}
  {%- if existing_type is not none and existing_type != 'table' -%}
      {{ adapter.drop(schema, identifier, existing_type) }}
  {%- endif -%}

  -- build model
  {% if partition_date is none %}
      {% set result = adapter.execute_model(model, 'table') %}
  {% else %}
      {% if existing_type is none %}
          {# TODO : Test that this works if a non dp-table is changed to a dp-table #}
          {{ adapter.make_date_partitioned_table(schema, identifier) }}
      {% endif %}
      {% set result = adapter.execute_model(model, 'table', decorator=partition_date) %}
  {% endif %}

  {{ store_result('main', status=result) }}

{% endmaterialization %}

{% materialization incremental, adapter='bigquery' -%}

  {{ exceptions.materialization_not_available(model, 'bigquery') }}

{% endmaterialization %}
