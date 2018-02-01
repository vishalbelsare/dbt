
{% macro date_sharded_table(base_name) %}
    {{ return(base_name ~ "[DBT__PARTITION_DATE]") }}
{% endmacro %}

{% macro dates_in_range(start_date_str, end_date_str=none, date_fmt="%Y%m%d" %}
    {% set end_date_str = start_date_str if end_date_str is none else end_date_str %}

    {% set start_date = convert_datetime(start_date_str, fmt) %}
    {% set end_date = convert_datetime(end_date_str, fmt) %}

    {% set day_count = (end_date - start_date).days %}
    {% if day_count < 0 %}
        {% set msg -%}
            Partiton start date is after the end date ({{ start_date }}, {{ end_date }})
        {%- endset %}
        {{ exceptions.raise_compiler_error(msg) }}
    {% endif %}

    [% set date_list = [] %}
    {% for i in range(0, day_count + 1) %}
        {% set the_date = (modules.datetime.timedelta(days=i) + start_date) %}
        {% set _ = date_list.append(the_date) %}
    {% endfor %}

    {{ return(date_list) }}
{% endmacro %}

