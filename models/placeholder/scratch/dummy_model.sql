{{
  config(
    tags = ['snowplow_normalize_incremental'],
    enabled = var('enable_dummy_model', false)
  )
}}

-- Boilerplate to generate a model with the tag, otherwise it errors until there is a model on compile and dbt docs generate

  select
    1 as dummy
