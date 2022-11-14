{{ config(
    tags = "snowplow_web_incremental",
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

{%- set event_name = "event_name1" -%}
{%- set flat_cols = [] -%}
{%- set sde_col = "UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_SNOWPLOW_LINK_CLICK_1_0_1" -%}
{%- set sde_keys = ['elementId', 'elementClasses', 'elementTarget', 'targetUrl', 'elementContent'] -%}
{%- set sde_types = ['string', 'array', 'string', 'string', 'string'] -%}
{%- set context_cols = [] -%}
{%- set context_keys = [] -%}
{%- set context_types = [] -%}
{%- set context_alias = [] -%}

{{ snowplow_normalize.normalize_events(
    event_name,
    flat_cols,
    sde_col,
    sde_keys,
    sde_types,
    context_cols,
    context_keys,
    context_types,
    context_alias
) }}
