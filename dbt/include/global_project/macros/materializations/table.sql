{% materialization table, default %}
  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}

  {%- set existing_relations = adapter.list_relations() -%}
  {%- set old_relation = adapter.get_relation(relations_list=existing_relations,
                                                 schema=schema, identifier=identifier) -%}
  {%- set target_relation = adapter.Relation.create(identifier=identifier, schema=schema, type='table') -%}
  {%- set tmp_relation = adapter.Relation.create(identifier=tmp_identifier,
                                                 schema=schema, type='table') -%}

  -- drop the temp relation if it exists for some reason
  {{ adapter.drop_relation(tmp_relation) }}

  -- setup: if the target relation already exists, truncate or drop it
  {% if non_destructive_mode -%}
    {% if old_relation.is_table -%}
      {{ adapter.truncate(schema, identifier) }}
    {% elif old_relation.is_view -%}
      {{ adapter.drop_relation(old_relation) }}
      {%- set old_relation = none -%}
    {%- endif %}
  {%- endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {%- if non_destructive_mode -%}
      {%- if old_relation is not none -%}
        {{ create_table_as(True, tmp_relation, sql) }}

        {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
        {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

        insert into {{ target_relation }} ({{ dest_cols_csv }}) (
          select {{ dest_cols_csv }}
          from {{ tmp_relation }}
        );
      {%- else -%}
        {{ create_table_as(False, target_relation, sql) }}
      {%- endif -%}
    {%- else -%}
      {{ create_table_as(False, tmp_relation, sql) }}
    {%- endif -%}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- cleanup
  {% if non_destructive_mode -%}
    -- noop
  {%- else -%}
    {{ drop_relation_if_exists(target_relation) }}
    {{ adapter.rename(schema, tmp_relation.identifier, target_relation.identifier) }}
  {%- endif %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
{% endmaterialization %}
