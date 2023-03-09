{% macro users_table(user_id_field = 'user_id', user_id_sde = '', user_id_context = '', user_cols = [], user_keys = [], user_types = [], user_id_alias = 'user_id', flat_cols = [], remove_new_event_check = false) %}
    {{ return(adapter.dispatch('users_table', 'snowplow_normalize')(user_id_field, user_id_sde, user_id_context, user_cols, user_keys, user_types, user_id_alias, flat_cols, remove_new_event_check)) }}
{% endmacro %}

{% macro snowflake__users_table(user_id_field = 'user_id', user_id_sde = '', user_id_context = '', user_cols = [], user_keys = [], user_types = [], user_id_alias = 'user_id', flat_cols = [], remove_new_event_check = false) %}
{# Remove down to major version for Snowflake columns, drop 2 last _X values #}
{%- set user_cols_clean = [] -%}
{%- for ind in range(user_cols|length) -%}
    {% do user_cols_clean.append('_'.join(user_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}

{# Raise a warining if both sde and context are provided as we only use one #}
{%- if user_id_sde != '' and user_id_context != '' -%}
{% do exceptions.warn("Snowplow: Both a user_id sde column and context column provided, only the sde column will be used.") %}
{%- endif -%}
{%- set snake_user_id =  snowplow_normalize.snakeify_case(user_id_alias) -%}

with defined_user_id as (
select
    {% if user_id_sde == '' and user_id_context == '' %}
        {{snowplow_normalize.snakeify_case(user_id_field)}} as {{ snake_user_id }} {# Snakeify case of standard column even in snowflake #}
    {% elif user_id_sde != '' %}
        {{ '_'.join(user_id_sde.split('_')[:-2]) }}:{{user_id_field}}::string as {{ snake_user_id }}
    {% elif user_id_context != '' %}
        {{ '_'.join(user_id_context.split('_')[:-2]) }}[0]:{{user_id_field}}::string as {{ snake_user_id }}
    {%- endif %}
    , collector_tstamp as latest_collector_tstamp
    -- Flat columns from event table
    {% if flat_cols|length > 0 %}
        {%- for col in flat_cols -%}
            , {{ col }}
        {% endfor -%}
    {%- endif -%}
    -- user column(s) from the event table
    {% if user_cols_clean|length > 0 %}
        {%- for col, col_ind in zip(user_cols_clean, range(user_cols_clean|length)) -%} {# Loop over each context column provided #}
            {%- for key, type in zip(user_keys[col_ind], user_types[col_ind]) -%} {# Loop over the keys in each column #}
                , {{ col }}[0]:{{ key }}::{{ type }} as {{ snowplow_normalize.snakeify_case(key) }}
            {% endfor -%}
        {%- endfor -%}
    {%- endif %}
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    1 = 1
    {% if not remove_new_event_check %}
        and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
    {%- endif -%}
)

{# Ensure only latest record is upserted into the table #}
select
    *
from
    defined_user_id
where
    {{ snake_user_id }} is not null
qualify
    row_number() over (partition by {{ snake_user_id }} order by latest_collector_tstamp desc) = 1
{% endmacro %}


{% macro bigquery__users_table(user_id_field = 'user_id', user_id_sde = '', user_id_context = '', user_cols = [], user_keys = [], user_types = [], user_id_alias = 'user_id', flat_cols = [], remove_new_event_check = false) %}
{# Remove down to major version for bigquery combine columns macro, drop 2 last _X values #}
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
{% set user_id_field = snowplow_normalize.snakeify_case(user_id_field) %}

{# Raise a warining if both sde and context are provided as we only use one #}
{%- if user_id_sde != '' and user_id_context != '' -%}
{% do exceptions.warn("Snowplow: Both a user_id sde column and context column provided, only the sde column will be used.") %}
{%- endif -%}

{%- set snake_user_id =  snowplow_normalize.snakeify_case(user_id_alias) -%}


with defined_user_id as (
    select
        {% if user_id_sde == '' and user_id_context == ''%}
            {{snowplow_normalize.snakeify_case(user_id_field)}} as {{ snake_user_id }}
        {% elif user_id_sde != '' %}
        {# Coalesce the sde column for the custom user_id field  #}
            {%- set user_id_sde_coal = snowplow_utils.combine_column_versions(
                                        relation=ref('snowplow_normalize_base_events_this_run'),
                                        column_prefix= user_id_sde.lower(),
                                        include_field_alias = False,
                                        required_fields = [ user_id_field ]
                                        ) -%}
            {{ user_id_sde_coal[0] }} as {{ snake_user_id }}

        {% elif user_id_context != '' %}
        {# Coalesce the context column for the custom user_id field  #}
            {%- set user_id_cont_coal = snowplow_utils.combine_column_versions(
                                        relation=ref('snowplow_normalize_base_events_this_run'),
                                        column_prefix= user_id_context.lower(),
                                        include_field_alias = False,
                                        required_fields = [ user_id_field ]
                                        ) -%}
            {{ user_id_cont_coal[0] }} as {{ snake_user_id }}
        {%- endif %}
        , collector_tstamp as latest_collector_tstamp
        -- Flat columns from event table
        {% if flat_cols|length > 0 %}
            {%- for col in flat_cols -%}
                , {{ col }}
            {% endfor -%}
        {%- endif -%}
        -- user column(s) from the event table
        {% if user_cols|length > 0 %}
            {%- for col, col_ind in zip(user_cols_clean, range(user_cols|length)) -%}  {# Loop over each context column, getting the coalesced version#}
                {%- set user_cols_list = snowplow_utils.combine_column_versions(
                                            relation=ref('snowplow_normalize_base_events_this_run'),
                                            column_prefix=col.lower(),
                                            include_field_alias = True,
                                            required_fields = user_keys_clean[col_ind]
                                            ) -%}
                {% for field in user_cols_list %} {# Loop over each field in the column, alias provided by macro #}
                    , {{field}}
                {%- endfor -%}
            {%- endfor -%}
        {%- endif %}
    from
        {{ ref('snowplow_normalize_base_events_this_run') }}
    where
        1 = 1
        {% if not remove_new_event_check %}
            and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
        {%- endif -%}
),

{# Order data to get the latest data having rn = 1 #}
users_ordering as (
    select
        a.*
        , row_number() over (partition by {{ snake_user_id }} order by latest_collector_tstamp desc) as rn
    from
        defined_user_id a
    where
        {{ snake_user_id }} is not null
)

{# Ensure only latest record is upserted into the table #}
select
    * except (rn)
from
    users_ordering
where
    rn = 1
{% endmacro %}

{% macro databricks__users_table(user_id_field = 'user_id', user_id_sde = '', user_id_context = '', user_cols = [], user_keys = [], user_types = [], user_id_alias = 'user_id', flat_cols = [], remove_new_event_check = false) %}
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
{% set user_id_field = snowplow_normalize.snakeify_case(user_id_field) %}

{# Raise a warining if both sde and context are provided as we only use one #}
{%- if user_id_sde != '' and user_id_context != '' -%}
{% do exceptions.warn("Snowplow: Both a user_id sde column and context column provided, only the sde column will be used.") %}
{%- endif -%}

{%- set snake_user_id =  snowplow_normalize.snakeify_case(user_id_alias) -%}

with defined_user_id as (
    select
        {% if user_id_sde == '' and user_id_context == ''%}
            {{ user_id_field }} as {{ snake_user_id }}
        {% elif user_id_sde != '' %}
            {{ '_'.join(user_id_sde.split('_')[:-2]) }}.{{ user_id_field }} as {{ snake_user_id }}
        {% elif user_id_context != '' %}
            {{ '_'.join(user_id_context.split('_')[:-2]) }}[0].{{ user_id_field }} as {{ snake_user_id }}
        {%- endif %}
        , collector_tstamp as latest_collector_tstamp
        {% if target.type in ['databricks', 'spark'] -%}
            , DATE(collector_tstamp) as latest_collector_tstamp_date
        {%- endif %}
        -- Flat columns from event table
        {% if flat_cols|length > 0 %}
            {%- for col in flat_cols -%}
                , {{ col }}
            {% endfor -%}
        {%- endif -%}
        -- user column(s) from the event table
        {% if user_cols_clean|length > 0 %}
            {%- for col, col_ind in zip(user_cols_clean, range(user_cols_clean|length)) -%} {# Loop over each context column provided #}
                {%- for key in user_keys_clean[col_ind] -%} {# Loop over the keys in each column #}
                    , {{ col }}[0].{{ key }} as {{ key }}
                {% endfor -%}
            {%- endfor -%}
        {%- endif %}
    from
        {{ ref('snowplow_normalize_base_events_this_run') }}
    where
        1 = 1
        {% if not remove_new_event_check %}
            and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
        {%- endif -%}

),

{# Order data to get the latest data having rn = 1 #}
users_ordering as (
select
    a.*
    , row_number() over (partition by {{ snake_user_id }} order by latest_collector_tstamp desc) as rn
from
    defined_user_id a
where
    {{ snake_user_id }} is not null
)

{# Ensure only latest record is upserted into the table #}
select
    * except (rn)
from
    users_ordering
where
    rn = 1
{% endmacro %}
