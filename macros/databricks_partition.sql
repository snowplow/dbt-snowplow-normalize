{% macro databricks_partition() %}
  {{ return(adapter.dispatch('databricks_partition', 'snowplow_normalize')()) }}
{% endmacro %}

{% macro default__databricks_partition() %}


  {% set databricks_partition = var('snowplow__partition_key')~"_date" %}

  -- Log the databricks_partition

  {{ log("Using databricks_partition: " ~ databricks_partition , info=True )}}

  {{ return(databricks_partition) }}

{% endmacro %}
