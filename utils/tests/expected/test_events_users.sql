{{ config(
    tags = "snowplow_normalize_incremental",
    materialized = "incremental",
    unique_key = "custom_user_id_alias",
    upsert_date_key = "latest_collector_tstamp",
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "latest_collector_tstamp",
      "data_type": "timestamp"
    }, databricks_val='latest_collector_tstamp_date'),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize=true
) }}

{%- set user_flat_cols = ['domain_userid', 'app_id', 'refr_urlpath'] -%}
{%- set user_cols = ['CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1_0_0', 'CONTEXTS_COM_IAB_SNOWPLOW_SPIDERS_AND_ROBOTS_1_0_0'] -%}
{%- set user_keys = [['useragentFamily', 'useragentMajor', 'useragentMinor', 'useragentPatch', 'useragentVersion', 'osFamily', 'osMajor', 'osMinor', 'osPatch', 'osPatchMinor', 'osVersion', 'deviceFamily'], ['spiderOrRobot', 'category', 'reason', 'primaryImpact']] -%}
{%- set user_types = [['string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string'], ['boolean', 'string', 'string', 'string']] -%}

{{ snowplow_normalize.users_table(
    'userId',
    'UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0',
    'CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0',
    user_cols,
    user_keys,
    user_types,
    'custom_user_id_alias',
    user_flat_cols
) }}
