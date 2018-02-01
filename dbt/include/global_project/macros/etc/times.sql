
{% macro convert_datetime(date_str, date_fmt) %}

  {% set error_msg -%}
      The provided partition date '{{ date_str }}' does not match the expected format '{{ date_fmt }}'
  {%- endset %}

  {% set res = try_or_compiler_error(error_msg, modules.datetime.datetime.strptime, date_str.strip(), date_fmt) %}
  {{ return(res) }}

{% endmacro %}

