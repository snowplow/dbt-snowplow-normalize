{{ config(
    tags = "snowplow_normalize_incremental",
    materialized = var("snowplow__incremental_materialization", "snowplow_incremental"),
    unique_key = "event_id",
    upsert_date_key = "collector_tstamp",
    partition_by = snowplow_utils.get_partition_by(bigquery_partition_by={
      "field": "collector_tstamp",
      "data_type": "timestamp"
    }, databricks_partition_by='collector_tstamp_date'),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    }
) }}

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    , 'event_name1' as event_name
    , 'itsaprefix_event_name1_1' as event_table_name
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name = 'event_name1'
    and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}

UNION ALL

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    , 'event_name2' as event_name
    , 'custom_table_name2_1' as event_table_name
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name = 'event_name2'
    and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}

UNION ALL

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    , 'event_name3' as event_name
    , 'custom_table_name3_2' as event_table_name
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name = 'event_name3'
    and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}

UNION ALL

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    , 'event_name4' as event_name
    , 'custom_table_name4_1' as event_table_name
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name = 'event_name4'
    and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
