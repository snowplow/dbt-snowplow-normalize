{{
  config(
    materialized='incremental',
    full_refresh=snowplow_normalize.allow_refresh()
  )
}}

-- Boilerplate to generate table.
-- Table updated as part of end-run hook

{{ snowplow_utils.base_create_snowplow_incremental_manifest() }}
