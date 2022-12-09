{{ config(
    tags = "snowplow_normalize_incremental",
) }}

select 1 as dummy from {{ ref ('snowplow_normalize_base_events_this_run')}}
