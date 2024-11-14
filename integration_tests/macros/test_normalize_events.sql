{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{# This tests the output of a dummy set of inputs to the normalize events macro to ensure that it returns what we expect to come out does.
This doesn't run on any actual data, we are just comparing the sql that is generated - removing whitespace to allow for changes to that.
Note that we have to pass the test = true argument for this to work without having to create all the manifest and event limits table.

It runs 9 tests:
1) Just flat columns, no sde or context
2) (1) plus an SDE
3) (2) plus an SDE alias
4) (2) + 1 context
5) (2) + 2 contexts
6) (5) + context aliases
7) (1) + 2 contexts
8) (7) + another base event
9) (7) + 2 SDEs

#}

{% macro test_normalize_events() %}

    {{ return(adapter.dispatch('test_normalize_events', 'snowplow_normalize_integration_tests')()) }}

{% endmacro %}

{% macro bigquery__test_normalize_events() %}

    {% set expected_dict = {
        "flat_cols_only" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_cols" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , coalesce(unstruct_event_test_1_0_1.test_class, unstruct_event_test_1_0_0.test_class) as test_class , coalesce(unstruct_event_test_1_0_1.test_id, unstruct_event_test_1_0_0.test_id) as test_id -- context column(s) from the event table from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_cols_w_alias" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , coalesce(unstruct_event_test_1_0_1.test_class, unstruct_event_test_1_0_0.test_class) as my_alias_test_class , coalesce(unstruct_event_test_1_0_1.test_id, unstruct_event_test_1_0_0.test_id) as my_alias_test_id -- context column(s) from the event table from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_1_context" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , coalesce(unstruct_event_test_1_0_1.test_class, unstruct_event_test_1_0_0.test_class) as test_class , coalesce(unstruct_event_test_1_0_1.test_id, unstruct_event_test_1_0_0.test_id) as test_id -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , coalesce(unstruct_event_test_1_0_1.test_class, unstruct_event_test_1_0_0.test_class) as test_class , coalesce(unstruct_event_test_1_0_1.test_id, unstruct_event_test_1_0_0.test_id) as test_id -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context_w_alias" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , coalesce(unstruct_event_test_1_0_1.test_class, unstruct_event_test_1_0_0.test_class) as test_class , coalesce(unstruct_event_test_1_0_1.test_id, unstruct_event_test_1_0_0.test_id) as test_id -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as test1_context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as test1_context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as test2_context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as test2_context_test_id2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "context_only" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "multiple_base_events" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name','page_ping')",
        "multiple_sde_events" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , coalesce(unstruct_event_test_1_0_1.test_class, unstruct_event_test_1_0_0.test_class) as test1_test_class , coalesce(unstruct_event_test_1_0_1.test_id, unstruct_event_test_1_0_0.test_id) as test1_test_id , coalesce(unstruct_event_test2_1_0_1.test_word, unstruct_event_test2_1_0_0.test_word) as test2_test_word , coalesce(unstruct_event_test2_1_0_1.test_idea, unstruct_event_test2_1_0_0.test_idea) as test2_test_idea -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')"
    } %}



    {% set results_dict ={
        "flat_cols_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_cols" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_cols_w_alias" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], ['my_alias'], [], [], [], [], true).split()|join(' '),
        "sde_plus_1_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], [], true).split()|join(' '),
        "sde_plus_2_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "sde_plus_2_context_w_alias" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], ['test1', 'test2'], true).split()|join(' '),
        "context_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_base_events" : snowplow_normalize.normalize_events(['event_name', 'page_ping'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_sde_events" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1', 'UNSTRUCT_EVENT_TEST2_1_0_1'], [['testId', 'testClass'], ['testWord', 'testIdea']], [['number', 'string']], ['test1', 'test2'], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' ')
        }
    %}


    {# {{ print(results_dict['flat_cols_only'])}} #}
    {# {{ print(results_dict['sde_plus_cols'])}} #}
    {# {{ print(results_dict['sde_plus_cols_w_alias'])}} #}
    {# {{ print(results_dict['sde_plus_1_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context_w_alias'])}} #}
    {# {{ print(results_dict['context_only'])}} #}
    {# {{ print(results_dict['multiple_base_events'])}} #}
    {# {{ print(results_dict['multiple_sde_events'])}} #}


    {{ dbt_unittest.assert_dict_equals(expected_dict, results_dict) }}


{% endmacro %}


{% macro databricks__test_normalize_events() %}

    {% set expected_dict = {
        "flat_cols_only" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table from `"~target.catalog~"`."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_cols" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table from `"~target.catalog~"`."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_cols_w_alias" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as my_alias_test_id , UNSTRUCT_EVENT_TEST_1.test_class as my_alias_test_class -- context column(s) from the event table from `"~target.catalog~"`."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_1_context" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class from `"~target.catalog~"`."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from `"~target.catalog~"`."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context_w_alias" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as test1_context_test_id , CONTEXTS_TEST_1[0].context_test_class as test1_context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as test2_context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as test2_context_test_class2 from `"~target.catalog~"`."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "context_only" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from `"~target.catalog~"`."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "multiple_base_events" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from `"~target.catalog~"`."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name','page_ping')",
        "multiple_sde_events" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test1_test_id , UNSTRUCT_EVENT_TEST_1.test_class as test1_test_class , UNSTRUCT_EVENT_TEST2_1.test_word as test2_test_word , UNSTRUCT_EVENT_TEST2_1.test_idea as test2_test_idea -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from `"~target.catalog~"`."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')"
    } %}

    {% set results_dict ={
        "flat_cols_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_cols" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_cols_w_alias" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], ['my_alias'], [], [], [], [], true).split()|join(' '),
        "sde_plus_1_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], [], true).split()|join(' '),
        "sde_plus_2_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "sde_plus_2_context_w_alias" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], ['test1', 'test2'], true).split()|join(' '),
        "context_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_base_events" : snowplow_normalize.normalize_events(['event_name', 'page_ping'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_sde_events" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1', 'UNSTRUCT_EVENT_TEST2_1_0_1'], [['testId', 'testClass'], ['testWord', 'testIdea']], [['number', 'string']], ['test1', 'test2'], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' ')
        }
    %}


    {# {{ print(results_dict['flat_cols_only'])}} #}
    {# {{ print(results_dict['sde_plus_cols'])}} #}
    {# {{ print(results_dict['sde_plus_cols_w_alias'])}} #}
    {# {{ print(results_dict['sde_plus_1_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context_w_alias'])}} #}
    {# {{ print(results_dict['context_only'])}} #}
    {# {{ print(results_dict['multiple_base_events'])}} #}
    {# {{ print(results_dict['multiple_sde_events'])}} #}


    {{ dbt_unittest.assert_dict_equals(expected_dict, results_dict) }}


{% endmacro %}


{% macro spark__test_normalize_events() %}

    -- Main difference here is that spark doesnt need the catalog in the from clause

    {% set expected_dict = {
        "flat_cols_only" : "select event_id , collector_tstamp , DATE(" ~ var('snowplow__partition_tstamp') ~ ") as " ~ var('snowplow__partition_tstamp') ~ "_date -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_cols" : "select event_id , collector_tstamp , DATE(" ~ var('snowplow__partition_tstamp') ~ ") as " ~ var('snowplow__partition_tstamp') ~ "_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_cols_w_alias" : "select event_id , collector_tstamp , DATE(" ~ var('snowplow__partition_tstamp') ~ ") as " ~ var('snowplow__partition_tstamp') ~ "_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as my_alias_test_id , UNSTRUCT_EVENT_TEST_1.test_class as my_alias_test_class -- context column(s) from the event table from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_1_context" : "select event_id , collector_tstamp , DATE(" ~ var('snowplow__partition_tstamp') ~ ") as " ~ var('snowplow__partition_tstamp') ~ "_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context" : "select event_id , collector_tstamp , DATE(" ~ var('snowplow__partition_tstamp') ~ ") as " ~ var('snowplow__partition_tstamp') ~ "_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context_w_alias" : "select event_id , collector_tstamp , DATE(" ~ var('snowplow__partition_tstamp') ~ ") as " ~ var('snowplow__partition_tstamp') ~ "_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as test1_context_test_id , CONTEXTS_TEST_1[0].context_test_class as test1_context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as test2_context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as test2_context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "context_only" : "select event_id , collector_tstamp , DATE(" ~ var('snowplow__partition_tstamp') ~ ") as " ~ var('snowplow__partition_tstamp') ~ "_date -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "multiple_base_events" : "select event_id , collector_tstamp , DATE(" ~ var('snowplow__partition_tstamp') ~ ") as " ~ var('snowplow__partition_tstamp') ~ "_date -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name','page_ping')",
        "multiple_sde_events" : "select event_id , collector_tstamp , DATE(" ~ var('snowplow__partition_tstamp') ~ ") as " ~ var('snowplow__partition_tstamp') ~ "_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test1_test_id , UNSTRUCT_EVENT_TEST_1.test_class as test1_test_class , UNSTRUCT_EVENT_TEST2_1.test_word as test2_test_word , UNSTRUCT_EVENT_TEST2_1.test_idea as test2_test_idea -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')"
    } %}

    {% set results_dict ={
        "flat_cols_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_cols" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_cols_w_alias" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], ['my_alias'], [], [], [], [], true).split()|join(' '),
        "sde_plus_1_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], [], true).split()|join(' '),
        "sde_plus_2_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "sde_plus_2_context_w_alias" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], ['test1', 'test2'], true).split()|join(' '),
        "context_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_base_events" : snowplow_normalize.normalize_events(['event_name', 'page_ping'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_sde_events" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1', 'UNSTRUCT_EVENT_TEST2_1_0_1'], [['testId', 'testClass'], ['testWord', 'testIdea']], [['number', 'string']], ['test1', 'test2'], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' ')
        }
    %}


    {# {{ print(results_dict['flat_cols_only'])}} #}
    {# {{ print(results_dict['sde_plus_cols'])}} #}
    {# {{ print(results_dict['sde_plus_cols_w_alias'])}} #}
    {# {{ print(results_dict['sde_plus_1_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context_w_alias'])}} #}
    {# {{ print(results_dict['context_only'])}} #}
    {# {{ print(results_dict['multiple_base_events'])}} #}
    {# {{ print(results_dict['multiple_sde_events'])}} #}


    {{ dbt_unittest.assert_dict_equals(expected_dict, results_dict) }}


{% endmacro %}


{% macro snowflake__test_normalize_events() %}

    {% set expected_dict = {
        "flat_cols_only" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_cols" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1:testId::string as test_id , UNSTRUCT_EVENT_TEST_1:testClass::boolean as test_class -- context column(s) from the event table from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_cols_w_alias" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1:testId::string as my_alias_test_id , UNSTRUCT_EVENT_TEST_1:testClass::boolean as my_alias_test_class -- context column(s) from the event table from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_1_context" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1:testId::string as test_id , UNSTRUCT_EVENT_TEST_1:testClass::boolean as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::string as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::integer as context_test_class from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1:testId::string as test_id , UNSTRUCT_EVENT_TEST_1:testClass::boolean as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXTS_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXTS_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context_w_alias" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1:testId::string as test_id , UNSTRUCT_EVENT_TEST_1:testClass::boolean as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as test1_context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as test1_context_test_class , CONTEXTS_TEST2_1[0]:contextTestId2::interger as test2_context_test_id2 , CONTEXTS_TEST2_1[0]:contextTestClass2::string as test2_context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "context_only" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXTS_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXTS_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "multiple_base_events" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXTS_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXTS_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name','page_ping')",
        "multiple_sde_events" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1:testId::number as test1_test_id , UNSTRUCT_EVENT_TEST_1:testClass::string as test1_test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXTS_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXTS_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')"
    } %}

   {% set results_dict ={
        "flat_cols_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_cols" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_cols_w_alias" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], ['my_alias'], [], [], [], [], true).split()|join(' '),
        "sde_plus_1_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], [], true).split()|join(' '),
        "sde_plus_2_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "sde_plus_2_context_w_alias" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], ['test1', 'test2'], true).split()|join(' '),
        "context_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_base_events" : snowplow_normalize.normalize_events(['event_name', 'page_ping'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_sde_events" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1', 'UNSTRUCT_EVENT_TEST2_1_0_1'], [['testId', 'testClass'], ['testWord', 'testIdea']], [['number', 'string']], ['test1', 'test2'], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' ')
        }
    %}


    {# {{ print(results_dict['flat_cols_only'])}} #}
    {# {{ print(results_dict['sde_plus_cols'])}} #}
    {# {{ print(results_dict['sde_plus_cols_w_alias'])}} #}
    {# {{ print(results_dict['sde_plus_1_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context_w_alias'])}} #}
    {# {{ print(results_dict['context_only'])}} #}
    {# {{ print(results_dict['multiple_base_events'])}} #}
    {# {{ print(results_dict['multiple_sde_events'])}} #}


    {{ dbt_unittest.assert_dict_equals(expected_dict, results_dict) }}


{% endmacro %}
