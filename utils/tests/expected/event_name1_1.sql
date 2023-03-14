{{ config(
    tags = "snowplow_normalize_incremental",
    materialized = "incremental",
    unique_key = "event_id",
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

{%- set event_names = ['event_name1'] -%}
{%- set flat_cols = ['app_id', 'domain_userid'] -%}
{%- set sde_cols = ['UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_SNOWPLOW_LINK_CLICK_1_0_1'] -%}
{%- set sde_keys = [['elementId', 'elementClasses', 'elementTarget', 'targetUrl', 'elementContent']] -%}
{%- set sde_types = [['string', 'array', 'string', 'string', 'string']] -%}
{%- set sde_aliases = [] -%}
{%- set context_cols = [] -%}
{%- set context_keys = [] -%}
{%- set context_types = [] -%}
{%- set context_alias = [] -%}

{{ snowplow_normalize.normalize_events(
    event_names,
    flat_cols,
    sde_cols,
    sde_keys,
    sde_types,
    sde_aliases,
    context_cols,
    context_keys,
    context_types,
    context_alias
) }}
