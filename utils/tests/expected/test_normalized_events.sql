{{ config(
    tags = "snowplow_normalize_incremental",
    materialized = "incremental",
    unique_key = "unique_id",
    upsert_date_key = "collector_tstamp",
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "collector_tstamp",
      "data_type": "timestamp"
    }, databricks_val='collector_tstamp_date'),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize=true
) }}

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    , event_name
    , 'itsaprefix_event_name1_1' as event_table_name
    , event_id||'-'||'itsaprefix_event_name1_1' as unique_id
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name in ('event_name1')
    and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}

UNION ALL

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    , event_name
    , 'custom_table_name2_1' as event_table_name
    , event_id||'-'||'custom_table_name2_1' as unique_id
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name in ('event_name2')
    and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}

UNION ALL

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    , event_name
    , 'custom_table_name3_2' as event_table_name
    , event_id||'-'||'custom_table_name3_2' as unique_id
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name in ('event_name3')
    and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}

UNION ALL

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    , event_name
    , 'custom_table_name4_1' as event_table_name
    , event_id||'-'||'custom_table_name4_1' as unique_id
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name in ('event_name4')
    and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}

UNION ALL

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    , event_name
    , 'custom_table_name5_9' as event_table_name
    , event_id||'-'||'custom_table_name5_9' as unique_id
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name in ('event_name5','event_name6')
    and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}

UNION ALL

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    , event_name
    , 'custom_table_name6_6' as event_table_name
    , event_id||'-'||'custom_table_name6_6' as unique_id
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name in ('event_name7','event_name8')
    and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}

UNION ALL

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    , event_name
    , 'custom_table_name7_6' as event_table_name
    , event_id||'-'||'custom_table_name7_6' as unique_id
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name in ('event_name9','event_name10')
    and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
