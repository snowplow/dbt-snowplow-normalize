{% macro decompose_events(event_name, flat_cols = [], sde_col = '', sde_keys = [], sde_types = [], context_cols = [], context_keys = [], context_types = [], context_aliases = []) %}
    {{ return(adapter.dispatch('decompose_events')(event_name, flat_cols, sde_col, sde_keys, sde_types, context_cols, context_keys, context_types, context_aliases)) }}
{% endmacro %}

{% macro snowflake__decompose_events(event_name, flat_cols, sde_col, sde_keys, sde_types, context_cols, context_keys, context_types, context_aliases) %}

{# Remove down to major version for Snowflake columns, drop 2 last _X values #}
{%- set sde_col = '_'.join(sde_col.split('_')[:-2]) -%} 
{%- set context_cols_clean = [] -%}
{%- for ind in range(context_cols|length) -%}
    {% do context_cols_clean.append('_'.join(context_cols[ind].split('_')[:-2])) -%} 
{%- endfor -%}

select
    event_id
    , collector_tstamp
    -- Flat columns from event table
    {% if flat_cols|length > 0 %}
    {%- for col in flat_cols -%}
    , {{ col }}
    {% endfor -%}
    {%- endif -%}
    -- self describing events column from event table
    {% if sde_col != '' %}
    {%- for key, type in zip(sde_keys, sde_types) -%}
    , {{ sde_col }}:{{ key }}::{{ type }} as {{ key }}
    {% endfor -%}
    {%- endif -%}
    -- context column(s) from the event table
    {% if context_cols_clean|length > 0 %}
    {%- for col, col_ind in zip(context_cols_clean, range(context_cols_clean|length)) -%}
    {%- for key, type in zip(context_keys[col_ind], context_types[col_ind]) -%}
    {% if context_aliases|length > 0 -%}
    , {{ col }}[0]:{{ key }}::{{ type }} as {{ context_aliases[col_ind] }}_{{ key }}
    {% else -%}
    , {{ col }}[0]:{{ key }}::{{ type }} as {{ key }}
    {%- endif -%}
    {%- endfor -%}
    {%- endfor -%}
    {%- endif %}
from
    {{ ref('snowplow_web_base_events_this_run') }}
where
    event_name = '{{ event_name }}'
    and {{ snowplow_utils.is_run_with_new_events("snowplow_web") }}
{% endmacro %}


{% macro bigquery__decompose_events(event_name, flat_cols, sde_col, sde_keys, sde_types, context_cols, context_keys, context_types, context_aliases) %}
{# Replace keys with snake_case where needed #}
{% set re = modules.re %}
{% set camel_string = '(?<!^)(?=[A-Z])'%}
{%- set sde_keys_clean = [] -%} 
{%- set context_keys_clean = [] -%}
{%- for ind in range(sde_keys|length) -%}
    {% do sde_keys_clean.append(re.sub(camel_string, '_', sde_keys[ind]).lower()) -%} 
{%- endfor -%}
{%- for ind1 in range(context_keys|length) -%}
    {%- set context_key_clean = [] -%}
    {%- for ind2 in range(context_keys[ind1]|length) -%}
        {% do context_key_clean.append(re.sub(camel_string, '_', context_keys[ind1][ind2]).lower()) -%} 
    {%- endfor -%}
    {% do context_keys_clean.append(context_key_clean) -%} 
{%- endfor -%}


select
    event_id
    , collector_tstamp
    -- Flat columns from event table
    {% if flat_cols|length > 0 %}
    {%- for col in flat_cols -%}
    , {{ col }}
    {% endfor -%}
    {%- endif -%}
    -- self describing events column from event table
    {% if sde_col != '' %}
    {%- for key, type in zip(sde_keys_clean, sde_types) -%}
    , {{ sde_col }}.{{ key }} as {{ key }}
    {% endfor -%}
    {%- endif -%}
    -- context column(s) from the event table
    {% if context_cols|length > 0 %}
    {%- for col, col_ind in zip(context_cols, range(context_cols|length)) -%}
    {%- for key in context_keys_clean[col_ind] -%}
    {% if context_aliases|length > 0 -%}
    , {{ col }}[SAFE_OFFSET(0)].{{ key }} as {{ context_aliases[col_ind] }}_{{ key }}
    {% else -%}
    , {{ col }}[SAFE_OFFSET(0)].{{ key }} as {{ key }}
    {%- endif -%}
    {%- endfor -%}
    {%- endfor -%}
    {%- endif %}
from
    {{ ref('snowplow_web_base_events_this_run') }}
where
    event_name = '{{ event_name }}'
    and {{ snowplow_utils.is_run_with_new_events("snowplow_web") }}
{% endmacro %}

{% macro databricks__decompose_events(event_name, flat_cols, sde_col, sde_keys, sde_types, context_cols, context_keys, context_types, context_aliases) %}

{# Remove down to major version for Databricks columns, drop 2 last _X values #}
{%- set sde_col = '_'.join(sde_col.split('_')[:-2]) -%} 
{%- set context_cols_clean = [] -%}
{%- for ind in range(context_cols|length) -%}
    {% do context_cols_clean.append('_'.join(context_cols[ind].split('_')[:-2])) -%} 
{%- endfor -%}

{# Replace keys with snake_case where needed #}
{% set re = modules.re %}
{% set camel_string = '(?<!^)(?=[A-Z])'%}
{%- set sde_keys_clean = [] -%} 
{%- set context_keys_clean = [] -%}
{%- for ind in range(sde_keys|length) -%}
    {% do sde_keys_clean.append(re.sub(camel_string, '_', sde_keys[ind]).lower()) -%} 
{%- endfor -%}
{%- for ind1 in range(context_keys|length) -%}
    {%- set context_key_clean = [] -%}
    {%- for ind2 in range(context_keys[ind1]|length) -%}
        {% do context_key_clean.append(re.sub(camel_string, '_', context_keys[ind1][ind2]).lower()) -%} 
    {%- endfor -%}
    {% do context_keys_clean.append(context_key_clean) -%} 
{%- endfor -%}


select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    -- Flat columns from event table
    {% if flat_cols|length > 0 %}
    {%- for col in flat_cols -%}
    , {{ col }}
    {% endfor -%}
    {%- endif -%}
    -- self describing events column from event table
    {% if sde_col != '' %}
    {%- for key, type in zip(sde_keys_clean, sde_types) -%}
    , {{ sde_col }}.{{ key }} as {{ key }}
    {% endfor -%}
    {%- endif -%}
    -- context column(s) from the event table
    {% if context_cols_clean|length > 0 %}
    {%- for col, col_ind in zip(context_cols_clean, range(context_cols_clean|length)) -%}
    {%- for key in context_keys_clean[col_ind] -%}
    {% if context_aliases|length > 0 -%}
    , {{ col }}[0].{{ key }} as {{ context_aliases[col_ind] }}_{{ key }}
    {% else -%}
    , {{ col }}[0].{{ key }} as {{ key }}
    {%- endif -%}
    {%- endfor -%}
    {%- endfor -%}
    {%- endif %}
from
    {{ ref('snowplow_web_base_events_this_run') }}
where
    event_name = '{{ event_name }}'
    and {{ snowplow_utils.is_run_with_new_events("snowplow_web") }}
{% endmacro %}
