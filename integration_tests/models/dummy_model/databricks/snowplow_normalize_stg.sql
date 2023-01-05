-- we don't actually use this data at all, we just need the model so the graph can build
select
  *
from {{ ref('snowplow_norm_dummy_events') }}
