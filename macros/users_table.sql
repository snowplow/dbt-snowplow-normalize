{% macro users_table(user_cols = [], user_keys = [], user_types = [], remove_new_event_check = false) %}
    {{ return(adapter.dispatch('users_table', 'snowplow_normalize')(user_cols, user_keys, user_types, remove_new_event_check)) }}
{% endmacro %}

{% macro snowflake__users_table(user_cols, user_keys, user_types, remove_new_event_check) %}
{# Remove down to major version for Snowflake columns, drop 2 last _X values #}
{%- set user_cols_clean = [] -%}
{%- for ind in range(user_cols|length) -%}
    {% do user_cols_clean.append('_'.join(user_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}


select
    user_id
    , collector_tstamp as latest_collector_tstamp
    -- user column(s) from the event table
    {% if user_cols_clean|length > 0 %}
    {%- for col, col_ind in zip(user_cols_clean, range(user_cols_clean|length)) -%}
    {%- for key, type in zip(user_keys[col_ind], user_types[col_ind]) -%}
    , {{ col }}[0]:{{ key }}::{{ type }} as {{ snowplow_normalize.snakeify_case(key) }}
    {% endfor -%}
    {%- endfor -%}
    {%- endif %}
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    user_id is not null
    {% if not remove_new_event_check %}
        and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
    {%- endif -%}
qualify
    row_number() over (partition by user_id order by collector_tstamp desc) = 1
{% endmacro %}


{% macro bigquery__users_table(user_cols, user_keys, user_types, remove_new_event_check) %}
{# Replace keys with snake_case where needed #}
{%- set user_keys_clean = [] -%}
{%- for ind1 in range(user_keys|length) -%}
    {%- set user_key_clean = [] -%}
    {%- for ind2 in range(user_keys[ind1]|length) -%}
        {% do user_key_clean.append(snowplow_normalize.snakeify_case(user_keys[ind1][ind2])) -%}
    {%- endfor -%}
    {% do user_keys_clean.append(user_key_clean) -%}
{%- endfor -%}


with users_ordering as (
select
    user_id
    , collector_tstamp as latest_collector_tstamp
    -- user column(s) from the event table
    {% if user_cols|length > 0 %}
    {%- for col, col_ind in zip(user_cols, range(user_cols|length)) -%}
    {%- for key in user_keys_clean[col_ind] -%}
    , {{ col }}[SAFE_OFFSET(0)].{{ key }} as {{ key }}
    {% endfor -%}
    {%- endfor -%}
    {%- endif %}
    , row_number() over (partition by user_id order by collector_tstamp desc) as rn
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    user_id is not null
    {% if not remove_new_event_check %}
        and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
    {%- endif -%}
)

select
    * except (rn)
from
    users_ordering
where
    rn = 1
{% endmacro %}

{% macro databricks__users_table(user_cols, user_keys, user_types, remove_new_event_check) %}
{# Remove down to major version for Databricks columns, drop 2 last _X values #}
{%- set user_cols_clean = [] -%}
{%- for ind in range(user_cols|length) -%}
    {% do user_cols_clean.append('_'.join(user_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}

{# Replace keys with snake_case where needed #}

{%- set user_keys_clean = [] -%}
{%- for ind1 in range(user_keys|length) -%}
    {%- set user_key_clean = [] -%}
    {%- for ind2 in range(user_keys[ind1]|length) -%}
        {% do user_key_clean.append(snowplow_normalize.snakeify_case(user_keys[ind1][ind2])) -%}
    {%- endfor -%}
    {% do user_keys_clean.append(user_key_clean) -%}
{%- endfor -%}


with users_ordering as (
select
    user_id
    , collector_tstamp as latest_collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as latest_collector_tstamp_date
    {%- endif %}
    -- user column(s) from the event table
    {% if user_cols_clean|length > 0 %}
    {%- for col, col_ind in zip(user_cols_clean, range(user_cols_clean|length)) -%}
    {%- for key in user_keys_clean[col_ind] -%}
    , {{ col }}[0].{{ key }} as {{ key }}
    {% endfor -%}
    {%- endfor -%}
    {%- endif %}
    , row_number() over (partition by user_id order by collector_tstamp desc) as rn
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    user_id is not null
    {% if not remove_new_event_check %}
        and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
    {%- endif -%}
)

select
    * except (rn)
from
    users_ordering
where
    rn = 1
{% endmacro %}
