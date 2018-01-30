{% materialization view, adapter='bigquery' -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {%- if existing_type is not none -%}
    {%- if existing_type == 'table' and not flags.FULL_REFRESH -%}
      {# this is only intended for date partitioned tables, but we cant see that field in the context #}
      {% set error_message -%}
        Trying to create model '{{ identifier }}' as a view, but it already exists as a table.
        Either drop the '{{ schema }}.{{ identifier }}' table manually, or use --full-refresh
      {%- endset %}
      {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

    {{ adapter.drop(schema, identifier, existing_type) }}
  {%- endif -%}

  -- build model
  {% set result = adapter.execute_model(model, 'view') %}
  {{ store_result('main', status=result) }}

{%- endmaterialization %}


{% macro make_date_partitioned_table(model, partition_date, should_create, verbose=False) %}


  {% set start_date = partition_date.start_date %}
  {% set end_date = partition_date.end_date %}

  {% if should_create %}
      {{ adapter.make_date_partitioned_table(model.schema, model.name) }}
  {% endif %}

  {% set day_count = (end_date - start_date).days %}

  {% if day_count < 0 %}
    {% set msg -%}
        Partiton start date is after the end date ({{ start_date }}, {{ end_date }})
    {%- endset %}
    {{ exceptions.raise_compiler_error(msg, model) }}
  {% endif %}

  {% for i in range(0, day_count + 1) %}
    {% set the_day = (modules.datetime.timedelta(days=i) + start_date).strftime('%Y%m%d') %}
    {% if verbose %}
        {% set table_start_time = modules.datetime.datetime.now().strftime("%H:%M:%S") %}
        {{ log(table_start_time ~ ' | -> Running for day ' ~ the_day, info=True) }}
    {% endif %}

    {% set fixed_sql = model['injected_sql'] | replace('[DBT__PARTITION_DATE]', the_day) %}
    {% set _ = adapter.execute_model(model, 'table', fixed_sql, decorator=the_day) %}
  {% endfor %}

  {% if day_count == 0 %}
      {% set result_str = 'CREATED 1 PARTITION' %}
  {% else %}
      {% set result_str = 'CREATED ' ~ (day_count + 1) ~ ' PARTITIONS' %}
  {% endif %}

  {{ return(result_str) }}

{% endmacro %}

{% macro convert_datetime(date_str, date_fmt) %}

  {% set error_msg -%}
      The provided partition date '{{ date_str }}' does not match the expected format '{{ date_fmt }}'
  {%- endset %}

  {% set res = try_or_compiler_error(error_msg, modules.datetime.datetime.strptime, date_str.strip(), date_fmt) %}
  {{ return(res) }}

{% endmacro %}

{% macro get_partition_date(raw_partition_date, date_fmt) %}

    {% if not raw_partition_date %}
      {{ return({"start_date": none, "end_date": none, "is_partitioned": false}) }}
    {% endif %}

    {% set partition_range = (raw_partition_date | string).split(",") %}

    {% if (partition_range | length) == 1 %}
      {% set start_date = convert_datetime(partition_range[0], date_fmt) %}
      {% set end_date = start_date %}
    {% elif (partition_range | length) == 2 %}
      {% set start_date = convert_datetime(partition_range[0], date_fmt) %}
      {% set end_date = convert_datetime(partition_range[1], date_fmt) %}
    {% else %}
      {{ dbt.exceptions.raise_compiler_error("Invalid partition time. Expected format: {Start Date}[,{End Date}]. Got: " ~ raw_partition_date) }}
    {% endif %}

    {{ return({"start_date": start_date, "end_date": end_date, "is_partitioned": true}) }}

{% endmacro %}

{% materialization table, adapter='bigquery' -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {%- set verbose = config.get('verbose', False) -%}
  {%- set partition_date_fmt = config.get('partition_date_format') or '%Y%m%d' -%}
  {%- set partition_date = get_partition_date(config.get('partition_date'), partition_date_fmt) -%}

  {#
      Since dbt uses WRITE_TRUNCATE mode for tables, we only need to drop this thing
      if it is not a table. If it _is_ already a table, then we can overwrite it without downtime
  #}
  {%- if existing_type is not none and existing_type != 'table' -%}
      {{ adapter.drop(schema, identifier, existing_type) }}
  {%- endif -%}

  -- build model
  {% if partition_date.is_partitioned %}
      {% set result = make_date_partitioned_table(model, partition_date, (existing_type != 'table'), verbose) %}
  {% else %}
      {% set result = adapter.execute_model(model, 'table') %}
  {% endif %}

  {{ store_result('main', status=result) }}

{% endmaterialization %}

{% materialization incremental, adapter='bigquery' -%}

  {{ exceptions.materialization_not_available(model, 'bigquery') }}

{% endmaterialization %}
