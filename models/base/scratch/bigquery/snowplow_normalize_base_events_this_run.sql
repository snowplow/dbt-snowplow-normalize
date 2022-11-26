{{
  config(
    tags=["this_run"]
  )
}}

{%- set lower_limit, upper_limit, sesison_start_limit = snowplow_utils.return_base_new_event_limits(ref('snowplow_normalize_base_new_event_limits')) %}

-- without downstream joins, it's safe to dedupe by picking the first event_id found.
select
  array_agg(e order by e.collector_tstamp limit 1)[offset(0)].*

from (

  select
    a.*

  from {{ var('snowplow__events') }} as a

  where a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'a.dvce_created_tstamp') }}
  and a.collector_tstamp >= {{ lower_limit }}
  and a.collector_tstamp <= {{ upper_limit }}
  {% if var('snowplow__derived_tstamp_partitioned', true) and target.type == 'bigquery' | as_bool() %}
    and a.derived_tstamp >= {{ snowplow_utils.timestamp_add('hour', -1, lower_limit) }}
    and a.derived_tstamp <= {{ upper_limit }}
  {% endif %}
  and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}

) e
group by e.event_id
