{{ config(
    tags = "snowplow_normalize_incremental",
    materialized = "incremental",
    unique_key = "event_id",
    upsert_date_key = "collector_tstamp",
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field":  var("snowplow__partition_key"),
      "data_type": "timestamp"
    }, databricks_val=databricks_partition()),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize=true
) }}

{%- set event_names = ['event_name7', 'event_name8'] -%}
{%- set flat_cols = ['app_id', 'domain_userid'] -%}
{%- set sde_cols = ['UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_SNOWPLOW_LINK_CLICK_1_0_1', 'UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_MOBILE_DEEP_LINK_1_0_0'] -%}
{%- set sde_keys = [['elementId', 'elementClasses', 'elementTarget', 'targetUrl', 'elementContent'], ['url', 'referrer']] -%}
{%- set sde_types = [['string', 'array', 'string', 'string', 'string'], ['string', 'string']] -%}
{%- set sde_aliases = ['click', 'deep'] -%}
{%- set context_cols = ['CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1_0_0', 'CONTEXTS_COM_IAB_SNOWPLOW_SPIDERS_AND_ROBOTS_1_0_0', 'CONTEXTS_NL_BASJES_YAUAA_CONTEXT_1_0_1'] -%}
{%- set context_keys = [['useragentFamily', 'useragentMajor', 'useragentMinor', 'useragentPatch', 'useragentVersion', 'osFamily', 'osMajor', 'osMinor', 'osPatch', 'osPatchMinor', 'osVersion', 'deviceFamily'], ['spiderOrRobot', 'category', 'reason', 'primaryImpact'], ['deviceClass', 'deviceName', 'deviceBrand', 'deviceCpu', 'deviceCpuBits', 'deviceFirmwareVersion', 'deviceVersion', 'operatingSystemClass', 'operatingSystemName', 'operatingSystemVersion', 'operatingSystemNameVersion', 'operatingSystemVersionBuild', 'layoutEngineClass', 'layoutEngineName', 'layoutEngineVersion', 'layoutEngineVersionMajor', 'layoutEngineNameVersion', 'layoutEngineNameVersionMajor', 'layoutEngineBuild', 'agentClass', 'agentName', 'agentVersion', 'agentVersionMajor', 'agentNameVersion', 'agentNameVersionMajor', 'agentBuild', 'agentLanguage', 'agentLanguageCode', 'agentInformationEmail', 'agentInformationUrl', 'agentSecurity', 'agentUuid', 'webviewAppName', 'webviewAppVersion', 'webviewAppVersionMajor', 'webviewAppNameVersionMajor', 'facebookCarrier', 'facebookDeviceClass', 'facebookDeviceName', 'facebookDeviceVersion', 'facebookFBOP', 'facebookFBSS', 'facebookOperatingSystemName', 'facebookOperatingSystemVersion', 'anonymized', 'hackerAttackVector', 'hackerToolkit', 'koboAffiliate', 'koboPlatformId', 'iECompatibilityVersion', 'iECompatibilityVersionMajor', 'iECompatibilityNameVersion', 'iECompatibilityNameVersionMajor', 'carrier', 'gSAInstallationID', 'networkType', 'operatingSystemNameVersionMajor', 'operatingSystemVersionMajor']] -%}
{%- set context_types = [['string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string'], ['boolean', 'string', 'string', 'string'], ['string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string']] -%}
{%- set context_alias = ['ua', 'spiderbot', 'yauaa'] -%}

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
