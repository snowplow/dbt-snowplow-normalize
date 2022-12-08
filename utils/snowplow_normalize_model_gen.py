import sys
from functions.snowplow_model_gen_funcs import *

## NOTE ##
# vendorPrefix is not currently supported for prioritising specific registries
# Caching outside of each run is not currently support and as such cacheSize makes no difference

##############
# Parse args #
##############
args = parse_args(sys.argv[1:])

####################################
# Check running from dbt proj root #
####################################
if not os.path.isdir('models') or not os.path.isfile('dbt_project.yml'):
    raise FileNotFoundError('Not in a valid root dbt project folder, please run from the folder containing dbt_project.yml')


# Overwrite default verboseprint now we have flag
verboseprint = print if args.verbose else lambda *a, **k: None

#######################
# Load + Parse config #
#######################

# Load config file
config_path = args.config
if not os.path.exists(config_path):
    raise FileNotFoundError(f'File {config_path} not found.')

verboseprint('Loading config...')
with open(config_path, 'r') as f:
    config = json.load(f)

if not validate_json(config, schema = config_schema, validate = True):
    raise ValueError('Invalid config file format, run with flag --configHelp for more information.')

# Parse config values
filtered_events_table_name = config.get('config').get('filtered_events_table_name')
event_names = []
sde_urls = []
context_urls = []
flat_cols = []
context_aliases = []
table_names = []
versions = []
for event in config.get('events'):
    # Importantly, append None if it isn't provided
    event_names.append(event.get('event_name'))
    sde_urls.append(event.get('self_describing_event_schema'))
    context_urls.append(event.get('context_schemas'))
    flat_cols.append(event.get('event_columns'))
    context_aliases.append(event.get('context_aliases'))
    table_names.append(event.get('table_name'))
    versions.append(event.get('version'))

# Parse users
user_urls = config.get('users', {}).get('user_contexts')
user_id_column = config.get('users', {}).get('user_id', {}).get('id_column') or 'user_id'
user_id_sde = config.get('users', {}).get('user_id', {}).get('id_self_describing_event_schema')
user_id_context = config.get('users', {}).get('user_id', {}).get('id_context_schema')
if user_id_sde is not None:
    user_id_sde = 'UNSTRUCT_EVENT_' + url_to_column(urlparse(user_id_sde).path)
else:
    user_id_sde = ''
if user_id_context is not None:
    user_id_context = 'CONTEXTS_' + url_to_column(urlparse(user_id_context).path)
else:
    user_id_context = ''

# Set defaults if they don't exist
validate_schemas = config.get('config').get('overwrite') or True
overwrite = config.get('config').get('overwrite') or True
resolver_file_path = config.get('config').get('resolver_file_path')
models_folder = config.get('config').get('models_folder') or 'snowplow_normalized_events'
user_table_name = config.get('config').get('users_table_name') or 'snowplow_events_users'
models_prefix = config.get('config').get('models_prefix') or 'snowplow'

# Run Cleanup if required
if args.cleanUp:
    cleanup_models(event_names, sde_urls, versions, table_names, models_prefix, models_folder, user_table_name, filtered_events_table_name, args.dryRun)

model_names = generate_names(event_names, sde_urls, versions, table_names, models_prefix)

# Check for duplicate model names
seen = set()
dupes = []
for x in model_names + [filtered_events_table_name, user_table_name]:
    if x in seen:
        dupes.append(x)
    else:
        seen.add(x)

if len(dupes) > 0:
    raise KeyError(f'Configruation leads to duplicate event names, please remove the duplicates and try again. Duplicates: {dupes}')

#################################
# Load resolver and get schemas #
#################################

# Load the resolver config to get list of regestries, and validate the structure
if resolver_file_path != 'default':
    verboseprint('Loading resolver...')
    if not os.path.exists(resolver_file_path):
        raise FileNotFoundError(f'File {resolver_file_path} not found, to use default Iglu Central please set value to "default".')
    try:
        with open(resolver_file_path, 'r') as f:
            iglu_resolver_parsed = json.load(f)
    except json.decoder.JSONDecodeError:
        print(f'Error parsing resolver config at {resolver_file_path}, please ensure this a valid JSON file.')
        raise
else:
    iglu_resolver_parsed = default_resolver

if not validate_json(iglu_resolver_parsed.get('data'), schema = resolver_schema, validate = True):
    raise ValueError(f'Resolver config at {resolver_file_path} is not valid, see https://docs.snowplow.io/docs/pipeline-components-and-applications/iglu/iglu-resolver/ for more details.')

# Loop over all registries and get the priority and list of all schemas on that registry for comparison later, store api keys as well
verboseprint('Getting schema lists from registries...')
for repo in iglu_resolver_parsed.get('data').get('repositories'):
    # Get uri and netloc
    repo_uri = repo.get('connection').get('http').get('uri')
    parsed_uri = urlparse(repo_uri)
    repo_netloc = parsed_uri.netloc
    priority.append(repo.get('priority'))
    # Store the api key if it's needed, None if it doesn't exist
    repo_keys[repo_netloc] = repo.get('connection').get('http').get('apikey')
    # Get all schemas in each repo
    if repo_keys[repo_netloc] is None:
        repo_schemas = get_schema(parsed_uri.scheme + '://'+ repo_netloc + '/schemas', repo_keys)
    else:
        repo_schemas = get_schema(parsed_uri.scheme + '://'+ repo_netloc + '/api/schemas', repo_keys)
    schemas_list[repo_uri] = repo_schemas

# Organise list in order of priority
schemas_list = {x[0]: x[1] for _, x in sorted(zip(priority, schemas_list.items()))}

######################
# Produce each model #
######################
for i in range(len(event_names)):
    # Get info needed to generate filename
    event_name = event_names[i]
    sde_url = sde_urls[i]
    version = versions[i]
    table_name = table_names[i]
    sde_major_version = sde_url.split('-')[0][-1] if sde_url is not None else version if version is not None else '1'
    model_name = model_names[i]
    filename = os.path.join('models', models_folder,  model_name + '.sql')

    # Check if file already exists
    if not overwrite and os.path.exists(filename):
        verboseprint(f'Model {filename} already exists, skipping...')
        next

    # Continue to generate model
    verboseprint(f'Generating model {event_name}')
    context_url = context_urls[i]
    flat_col = flat_cols[i]
    # Remove columns already included
    flat_col = sorted(list(set(flat_col).difference({'event_id', 'collector_tstamp'})))
    context_alias = context_aliases[i]

    if sde_url is not None:
        # Parse the input URL then get parse and validate schemas for sde
        sde_url_cut = urlparse(sde_url).path
        sde_json = get_schema(parse_schema_url(sde_url, schemas_list, repo_keys), repo_keys)
        if not validate_json(sde_json, validate = validate_schemas, schemas_list = schemas_list, repo_keys = repo_keys):
            raise ValueError(f'Validation of schema {sde_url} failed.')
        # Generate final form data for insert into model
        sde_col = 'UNSTRUCT_EVENT_' + url_to_column(sde_url_cut)
        sde_keys = list(sde_json.get('properties').keys())
        sde_types = get_types(sde_json)
    else:
        sde_col = None
        sde_keys = None
        sde_types = None

    if context_url is not None:
        # Parse the input URL then get parse and validate schemas for contexts
        context_url_cut = [urlparse(url).path for url in context_url]
        context_jsons = [get_schema(parse_schema_url(url, schemas_list, repo_keys), repo_keys) for url in context_url]
        for i, context_json in enumerate(context_jsons):
            if not validate_json(context_json, validate = validate_schemas, schemas_list = schemas_list, repo_keys = repo_keys):
                raise ValueError(f'Validation of schema {context_url[i]} failed.')
        # Generate final form data for insert into model
        context_cols = ['CONTEXTS_' + url_to_column(url) for url in context_url_cut]
        context_keys = [list(context.get('properties').keys()) for context in context_jsons]
        context_types = [get_types(context) for context in context_jsons]
        if context_alias is None:
            context_alias = [context.get('self').get('name') for context in context_jsons]
    else:
        context_cols = None
        context_keys = None
        context_types = None

    # Write model string
    model_content = f"""{{{{ config(
    tags = "snowplow_normalize_incremental",
    materialized = var("snowplow__incremental_materialization", "snowplow_incremental"),
    unique_key = "event_id",
    upsert_date_key = "collector_tstamp",
    partition_by = snowplow_utils.get_partition_by(bigquery_partition_by={{
      "field": "collector_tstamp",
      "data_type": "timestamp"
    }}, databricks_partition_by='collector_tstamp_date'),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={{
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    }}
) }}}}

{{%- set event_name = "{event_name}" -%}}
{{%- set flat_cols = {flat_col or []} -%}}
{{%- set sde_col = "{sde_col or ""}" -%}}
{{%- set sde_keys = {sde_keys or []} -%}}
{{%- set sde_types = {sde_types or []} -%}}
{{%- set context_cols = {context_cols or []} -%}}
{{%- set context_keys = {context_keys or []} -%}}
{{%- set context_types = {context_types or []} -%}}
{{%- set context_alias = {context_alias or []} -%}}

{{{{ snowplow_normalize.normalize_events(
    event_name,
    flat_cols,
    sde_col,
    sde_keys,
    sde_types,
    context_cols,
    context_keys,
    context_types,
    context_alias
) }}}}
"""


    # Write out to file
    verboseprint(f'Model content for {model_name}, saving to {filename}:')
    verboseprint(model_content)
    if not args.dryRun:
        write_model_file(filename, model_content, overwrite = overwrite)


############################
# Produce all events model #
############################
if filtered_events_table_name is not None:
    verboseprint('Generating filtered events table model...')
    n_models = len(event_names)
    filtered_model_content = f"""{{{{ config(
    tags = "snowplow_normalize_incremental",
    materialized = var("snowplow__incremental_materialization", "snowplow_incremental"),
    unique_key = "event_id",
    upsert_date_key = "collector_tstamp",
    partition_by = snowplow_utils.get_partition_by(bigquery_partition_by={{
      "field": "collector_tstamp",
      "data_type": "timestamp"
    }}, databricks_partition_by='collector_tstamp_date'),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={{
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    }}
) }}}}
"""

    for n, (model, event_name) in enumerate(zip(model_names, event_names)):

        filtered_model_content += f"""
select
    event_id
    , collector_tstamp
    {{% if target.type in ['databricks', 'spark'] -%}}
    , DATE(collector_tstamp) as collector_tstamp_date
    {{%- endif %}}
    , '{event_name}' as event_name
    , '{model}' as event_table_name
from
    {{{{ ref('snowplow_normalize_base_events_this_run') }}}}
where
    event_name = '{event_name}'
    and {{{{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}}}
        """
        if n != n_models -1:
            filtered_model_content += """
UNION ALL
"""

    filename = os.path.join('models', models_folder, filtered_events_table_name + '.sql')
    verboseprint(f'Model content for {filtered_events_table_name}, saving to {filename}:')
    verboseprint(filtered_model_content)
    if not args.dryRun:
        write_model_file(filename, filtered_model_content, overwrite = overwrite)
else:
    verboseprint('No filtered events table model to generate...')


#######################
# Produce users model #
#######################
if user_urls is not None:
    verboseprint('Generating users table model...')
    user_url_cut = [urlparse(url).path for url in user_urls]
    user_jsons = [get_schema(parse_schema_url(url, schemas_list, repo_keys), repo_keys) for url in user_urls]
    for i, user_json in enumerate(user_jsons):
        if not validate_json(user_json, validate = validate_schemas, schemas_list = schemas_list, repo_keys = repo_keys):
            raise ValueError(f'Validation of schema {user_urls[i]} failed.')
    # Generate final form data for insert into model
    user_cols = ['CONTEXTS_' + url_to_column(url) for url in user_url_cut]
    user_keys = [list(user.get('properties').keys()) for user in user_jsons]
    user_types = [get_types(user) for user in user_jsons]

    users_model_content = f"""{{{{ config(
    tags = "snowplow_normalize_incremental",
    materialized = var("snowplow__incremental_materialization", "snowplow_incremental"),
    unique_key = "user_id",
    upsert_date_key = "latest_collector_tstamp",
    partition_by = snowplow_utils.get_partition_by(bigquery_partition_by={{
      "field": "latest_collector_tstamp",
      "data_type": "timestamp"
    }}, databricks_partition_by='latest_collector_tstamp_date'),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={{
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    }}
) }}}}

{{%- set user_cols = {user_cols or []} -%}}
{{%- set user_keys = {user_keys or []} -%}}
{{%- set user_types = {user_types or []} -%}}

{{{{ snowplow_normalize.users_table(
    '{user_id_column}',
    '{user_id_sde}',
    '{user_id_context}',
    user_cols,
    user_keys,
    user_types
) }}}}
"""

    filename = os.path.join('models', models_folder, user_table_name + '.sql')
    verboseprint(f'Model content for {user_table_name}, saving to {filename}:')
    verboseprint(users_model_content)
    if not args.dryRun:
        write_model_file(filename, users_model_content, overwrite = overwrite)
else:
    verboseprint('No users events table model to generate...')


verboseprint('Finished!')
