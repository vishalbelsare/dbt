
{{
    config(
        materialized='table',
        partition_date='2018-01-01,2018-01-03',
        partition_date_format='%Y-%m-%d',
        verbose=True
    )
}}

-- Hack to make sure our events models run first.
-- In practice, these would be source data
-- {{ ref('events_20180101') }}
-- {{ ref('events_20180102') }}
-- {{ ref('events_20180103') }}

select * from `{{ this.schema }}`.`{{ date_sharded_table('events_') }}`
