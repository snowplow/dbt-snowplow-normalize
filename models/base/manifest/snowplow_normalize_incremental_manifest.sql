{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    materialized='incremental',
    full_refresh=snowplow_normalize.allow_refresh()
  )
}}

-- Boilerplate to generate table.
-- Table updated as part of end-run hook

{{ snowplow_utils.base_create_snowplow_incremental_manifest() }}
