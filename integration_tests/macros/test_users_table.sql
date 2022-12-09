{# This tests the output of a dummy set of inputs to the user table macro to ensure that it returns what we expect to come out does.
This doesn't run on any actual data, we are just comparing the sql that is generated - removing whitespace to allow for changes to that.
Note that we have to pass the test = true argument for this to work without having to create all the manifest and event limits table.

It runs 2 tests:
1) A single context for the user
2) 2 contexts for the user

#}

{% macro test_users_table() %}

    {{ return(adapter.dispatch('test_users_table', 'snowplow_normalize_integration_tests')()) }}

{% endmacro %}

{% macro bigquery__test_users_table() %}

    {% set expected_dict = {
        "1_context" : "with users_ordering as ( select user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , CONTEXTS_TEST_1_0_0[SAFE_OFFSET(0)].context_test_id as context_test_id , CONTEXTS_TEST_1_0_0[SAFE_OFFSET(0)].context_test_class as context_test_class , row_number() over (partition by user_id order by collector_tstamp desc) as rn from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "2_context" : "with users_ordering as ( select user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , CONTEXTS_TEST_1_0_0[SAFE_OFFSET(0)].context_test_id as context_test_id , CONTEXTS_TEST_1_0_0[SAFE_OFFSET(0)].context_test_class as context_test_class , CONTEXT_TEST2_1_0_5[SAFE_OFFSET(0)].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1_0_5[SAFE_OFFSET(0)].context_test_class2 as context_test_class2 , row_number() over (partition by user_id order by collector_tstamp desc) as rn from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where user_id is not null ) select * except (rn) from users_ordering where rn = 1"
    } %}



    {% set results_dict ={
        "1_context" : snowplow_normalize.users_table(['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], true).split()|join(' '),
        "2_context" : snowplow_normalize.users_table(['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        }
    %}


    {# {{ print(results_dict['1_context'])}} #}
    {# {{ print(results_dict['2_context'])}} #}


    {{ dbt_unittest.assert_equals(expected_dict, results_dict) }}


{% endmacro %}


{% macro databricks__test_users_table() %}

    {% set expected_dict = {
        "1_context" : "with users_ordering as ( select user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , row_number() over (partition by user_id order by collector_tstamp desc) as rn from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "2_context" : "with users_ordering as ( select user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 , row_number() over (partition by user_id order by collector_tstamp desc) as rn from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where user_id is not null ) select * except (rn) from users_ordering where rn = 1"
    } %}



    {% set results_dict ={
        "1_context" : snowplow_normalize.users_table(['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], true).split()|join(' '),
        "2_context" : snowplow_normalize.users_table(['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        }
    %}


    {# {{ print(results_dict['1_context'])}} #}
    {# {{ print(results_dict['2_context'])}} #}


    {{ dbt_unittest.assert_equals(expected_dict, results_dict) }}


{% endmacro %}


{% macro snowflake__test_users_table() %}

   {% set expected_dict = {
        "1_context" : "select user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::string as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::integer as context_test_class from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where user_id is not null qualify row_number() over (partition by user_id order by collector_tstamp desc) = 1",
        "2_context" : "select user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where user_id is not null qualify row_number() over (partition by user_id order by collector_tstamp desc) = 1",
    } %}



    {% set results_dict ={
        "1_context" : snowplow_normalize.users_table(['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], true).split()|join(' '),
        "2_context" : snowplow_normalize.users_table(['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        }
    %}


    {# {{ print(results_dict['1_context'])}} #}
    {# {{ print(results_dict['2_context'])}} #}


    {{ dbt_unittest.assert_equals(expected_dict, results_dict) }}


{% endmacro %}
