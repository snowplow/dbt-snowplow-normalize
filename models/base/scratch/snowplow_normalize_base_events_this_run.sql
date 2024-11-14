{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

{%- set lower_limit, upper_limit, session_start_limit = snowplow_utils.return_base_new_event_limits(ref('snowplow_normalize_base_new_event_limits')) %}

with prep AS (

  select
      a.*

  {% if target.type not in ['databricks','snowflake','bigquery'] %}
    , row_number() over (partition by a.event_id order by a.collector_tstamp{% if target.type in ['databricks', 'spark'] -%}, a.dvce_sent_tstamp {%- endif %}) as rn
  {% endif %}

  from {{ var('snowplow__events') }} as a
  where
    {# dvce_sent_tstamp is an optional field and not all trackers/webhooks populate it, this means this filter needs to be optional #}
    {% if var("snowplow__days_late_allowed") == -1 %}
      1 = 1
    {% else %}
      a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'a.dvce_created_tstamp') }}
    {% endif %}
    and a.{{ var('snowplow__session_timestamp', 'collector_tstamp') }} >= {{ lower_limit }}
    and a.{{ var('snowplow__session_timestamp', 'collector_tstamp') }} <= {{ upper_limit }}
    {% if var('snowplow__derived_tstamp_partitioned', true) and target.type == 'bigquery' | as_bool() %}
      and a.derived_tstamp >= {{ snowplow_utils.timestamp_add('hour', -1, lower_limit) }}
      and a.derived_tstamp <= {{ upper_limit }}
    {% endif %}
    and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}
  {# We are doing the branching in order not to do the qualify in the case of spark, as it does not support it #}
  {% if target.type in ['databricks','snowflake','bigquery'] %}
  qualify row_number() over (partition by a.event_id order by a.collector_tstamp{% if target.type in ['databricks', 'spark'] -%}, a.etl_tstamp {%- endif %}) = 1
  {% endif %}
)

SELECT *
FROM prep
{% if target.type not in ['databricks','snowflake','bigquery'] %}
WHERE rn = 1
{% endif %}