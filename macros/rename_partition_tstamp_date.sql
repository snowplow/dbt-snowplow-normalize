{% macro rename_partition_tstamp_date() %}
  {{ return(adapter.dispatch('rename_partition_tstamp_date', 'snowplow_normalize')()) }}
{% endmacro %}

{% macro default__rename_partition_tstamp_date() %}


  {% set rename_partition_tstamp_date = var('snowplow__partition_tstamp')~"_date" %}

  {{ log("Rename partition to: " ~ rename_partition_tstamp_date)}}

  {{ return(rename_partition_tstamp_date) }}

{% endmacro %}
