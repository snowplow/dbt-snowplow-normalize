{# This tests the output of a dummy set of inputs to the snakeify case macro to ensure that it returns what we expect to come out does.
This doesn't run on any actual data, we are just comparing the text that is generated.

It runs 9 tests:
1) a single world, no change
2) already snake case, no change
3) pascal case
4) camel case, basic
5) camel case with multiple workds
6) camel case ending with a series of capitals
7) camel case with a series of capitals in the middle
8) camel case with hypthons
9) camel case with numbers


#}

{% macro test_snakeify_case() %}

    {{ return(adapter.dispatch('test_snakeify_case', 'snowplow_normalize_integration_tests')()) }}

{% endmacro %}

{% macro default__test_snakeify_case() %}

    {% set expected_dict = {
        "single_word" : "hello",
        "already_snake" : "hello_world",
        "pascal_case" : "hello_world",
        "camel_basic" : "hello_world",
        "camel_multi_word" : "hello_world_earth",
        "camel_end_with_caps" :"hello_world_earth",
        "camel_middle_caps" : "hello_world_earth",
        "camel_hypthons" : "hello_world_earth",
        "camel_numbers" : "hello_world23_earth"
    } %}

    {% set results_dict ={
        "single_word" : snowplow_normalize.snakeify_case('hello'),
        "already_snake" : snowplow_normalize.snakeify_case('hello_world'),
        "pascal_case" : snowplow_normalize.snakeify_case('HelloWorld'),
        "camel_basic" : snowplow_normalize.snakeify_case('helloWorld'),
        "camel_multi_word" : snowplow_normalize.snakeify_case('helloWorldEarth'),
        "camel_end_with_caps" : snowplow_normalize.snakeify_case('helloWorldEARTH'),
        "camel_middle_caps" : snowplow_normalize.snakeify_case('helloWORLDEarth'),
        "camel_middle_caps" : snowplow_normalize.snakeify_case('helloWORLDEarth'),
        "camel_hypthons" : snowplow_normalize.snakeify_case('helloWorld-Earth'),
        "camel_numbers" : snowplow_normalize.snakeify_case('helloWorld23Earth')
        }
    %}


    {{ dbt_unittest.assert_equals(expected_dict, results_dict) }}


{% endmacro %}
