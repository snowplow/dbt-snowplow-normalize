{# Take a string in camel/pascal case and make it snakecase #}
{% macro snakeify_case(text) %}
    {{ return(adapter.dispatch('snakeify_case', 'snowplow_normalize')(text)) }}
{% endmacro %}

{% macro default__snakeify_case(text) %}
    {%- set re = modules.re -%}
    {%- set camel_string1 = '([A-Z]+)([A-Z][a-z])'-%} {# Capitals followed by a lowercase  #}
    {%- set camel_string2 = '([a-z\d])([A-Z])'-%} {# lowercase followed by a capital #}
    {%- set replace_string = '\\1_\\2' -%}
    {%- set output_text = re.sub(camel_string2, replace_string, re.sub(camel_string1, replace_string, text)).replace('-', '_').lower() -%}
    {{- output_text -}}
{% endmacro %}
