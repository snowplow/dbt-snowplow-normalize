{{
  config(
    tags = ['snowplow_normalize_incremental']
  )
}}

-- Boilerplate to generate a model with the tag, otherwise it errors until there is a model on compile and dbt docs generate

  select
    1
