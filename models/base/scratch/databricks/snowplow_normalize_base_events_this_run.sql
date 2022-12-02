{{
  config(
    tags=["this_run"]
  )
}}

{%- set lower_limit, upper_limit, session_start_limit = snowplow_utils.return_base_new_event_limits(ref('snowplow_normalize_base_new_event_limits')) %}

select
    a.*

from {{ var('snowplow__events') }} as a

where a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'a.dvce_created_tstamp') }}
and a.collector_tstamp >= {{ lower_limit }}
and a.collector_tstamp <= {{ upper_limit }}
and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}

qualify row_number() over (partition by a.event_id order by a.collector_tstamp, a.etl_tstamp) = 1
