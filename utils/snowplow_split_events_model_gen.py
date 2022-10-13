import requests
import json
import os
import warnings
import jsonschema
import argparse
from urllib.parse import urlparse
from typing import Union

## NOTE ##
# vendorPrefix is not currently supported for prioritising specific registries
# caching outside of each run is not currently support and as such cacheSize makes no difference
# This file will not delete any existing models if you change the config, these will need to be removed manually.

##############
# Parse args #
##############
config_help = """
JSON Config file structure:
{
    "config":{
        "resolver_file_path": <required - string: relative path to your resolver config json>,
        "filtered_events_table_name": <optional - string: name of total events table, if not provided it will not be generated>,
        "users_table_name": <optional - string: name of users table, default events_users if user schema(s) provided>,
        "validate_schemas": <optional - boolean: if you want to validate schemas loaded from each iglu or not, default true>,
        "overwrite": <optional - boolean: overwrite exisiting model files or not, default true>,
        "models_folder": <optional - string: folder under models/ to place the models, default snowplow_split_events>
    },
    "events":[
        {
            "event_name": <required - string: name of the event type, value of the event_name column in your warehouse>,
            "event_columns": <optional (>=1 of) - list: list of strings of flat column names from the events table to include to include in the model>,
            "self_describing_event_schema": <optional (>=1 of) - string: `iglu:com.` type url for the self-desctibing event to include in the model>,
            "context_schemas": <optional (>=1 of) - list: list of strings of `iglu:com.` type url(s) for the context/entities to include in the model>,
            "context_aliases": <optional - list: list of strings of prefix to the column alias for context/entities>,
            "table_name": <optional - string: name of the model, default is the event_name and major version number>
        },
        {
            ...
        }
    ],
    "users": <optional - list: list of strings of schemas for your user contexts to add to your users table as columns, if not provided will not generate users model>
}"""

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description = 'Produce dbt model files for splitting your Snowplow events table into 1 table per event.')
parser.add_argument('config', help = 'relative path to your configuration file')
parser.add_argument('--version', action='version',
                    version='%(prog)s V0.0.1', help="Show program's version number and exit.")
parser.add_argument('-v', '--verbose', dest = 'verbose', action = 'store_true', default = False, help = 'Verbose flag for the running of the tool')
parser.add_argument('--dryRun', dest = 'dryRun', action = 'store_true', default = False, help ='Flag for a dry run (does not write to files).')
parser.add_argument('--configHelp', dest = 'configHelp', action = 'version', version = config_help, help ='Prints information relating to the structure of the config file.')

args = parser.parse_args()

####################################
# Check running from dbt proj root #
####################################
if not os.path.isdir('./models/') or not os.path.isfile('dbt_project.yml'):
    raise FileNotFoundError('Not in a valid root dbt project folder, please run from the folder containing dbt_project.yml')


#######################
# Functions + Lookups #
#######################
verboseprint = print if args.verbose else lambda *a, **k: None

def write_model_file(filename: str, model_code: str, overwrite: bool = True):
    """Write model code into a file

    Note that folders will be created if they do not exist as part of the filename, and existing files will be overwritten.

    Args:
        filename (str): The name of the file to write the code to, including path
        model_code (str): String to write into the file
        overwrite (bool): Overwrite the file if it already exists. Defaults to True
    """
    if not overwrite and os.path.exists:
        verboseprint(f'Model {filename} already exists, skipping...')
        pass
    else:
        verboseprint(f'Writing file {filename} ...')
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            f.write(model_code)

def get_types(jsonData: dict) -> list:
    """Get a list of types from a Snowplow schema

    Args:
        jsonData (dict): A parsed Snowplow self-describing event or entity schema

    Returns:
        list: A list of types for the properties in your schema
    """
    types = []
    for val in jsonData['properties'].values():
        if val.get('type') is not None:
            cur_type = val.get('type')
            types.append(max([cur_type] if isinstance(cur_type, str) else cur_type, key = lambda x: type_hierarchy[x]))
        elif val.get('enum') is not None:
            try:
                # "Check" the type that is in the list of options
                check_type = [float(option) for option in val.get('enum')]
                types.append('number')
            except ValueError:
                types.append('string')
        else:
            # Should never reach here as we validated the JSON but just incase
            raise ValueError(f'Excpted one of "type" or "enum" in property {val}')
    return types

def url_to_column(str: str) -> str:
    """convert url string to database column format

    Args:
        str (str): Input url

    Returns:
        str: Output column name cleaned of punctuation and replaced with underscores
    """
    return str.replace('/jsonschema', '', 1).replace('.', '_').replace('-', '_').replace('/', '_').upper()

def parse_schema_url(url: str) -> str:
    """Parse a schema URL and provide the true URL to GET request

    Args:
        url (string): the schema url to parse into a true url, should start with iglu: or http

    Raises:
        ValueError: If url does not start with the expected string
    
    Returns:
        str: A true URL that a GET request can be sent to
    """
    parsed_url = urlparse(url)
    if parsed_url.scheme == 'iglu':
        verboseprint(f'Identifying registry for iglu schema {url} ...')
        for registry, schemas in schemas_list.items():
            if url in schemas:
                if repo_keys[urlparse(registry).netloc] is not None:
                    schema_path = registry + '/api/schemas/' + parsed_url.path
                    return(schema_path)
                else:
                    schema_path = registry + '/schemas/' + parsed_url.path
                    return(schema_path) 
        raise ValueError(f'Schema {url} not found in any provided registry.')
    elif parsed_url.scheme == 'http':
        return(url)
    else:
        raise ValueError(f'Unexpected schema url scheme: {url} should be one of iglu, http.')

def get_schema(url: str) -> Union[dict, list]:
    """Return schema from url (using cache if available)

    Args:
        url (string): The URL to send a GET request to, using API key details if required
    Returns:
        Union[dict, list]: Returns the data formated literally
    """
    schema = schema_cache.get(url)
    if schema is None:
        verboseprint(f'Fetching schema {url} ...')
        parsed_url = urlparse(url)
        api_key = repo_keys.get(parsed_url.netloc)
        if api_key is None:
            schema = requests.get(url).text
        else:
            headers = {'apikey': api_key}
            schema = requests.get(url, headers=headers).text
        schema_cache[url] = schema
    else:
        verboseprint(f'Using cache for schema {url} ...')
    schema = json.loads(schema)
    return(schema)

def validate_json(jsonData: dict, schema: dict = None, validate: bool = True) -> bool:
    """Validates a JSON against a schema

    Args:
        jsonData (dict): A dictionary of the JSON data of the schema to validate
        schema (dict, optional): The schema to validate against. If provided will compare otherwise will look for a "schema" property of the jsonData. Defaults to None.
        validate (bool, optional): If validation should be run or not, function returns True if no valdiation is run. Defaults to True.

    Returns:
        bool: If the jsonData validated succfully against the schema or not
    """
    if validate:
        verboseprint('Validating JSON structure...')
        if schema is None:
            schema_url = jsonData.get('$schema') or jsonData.get('schema')
            parsed_schema = parse_schema_url(schema_url)
            schema = get_schema(parsed_schema)
        try:
            jsonschema.validate(instance=jsonData, schema=schema)
        except jsonschema.exceptions.ValidationError as err:
            warnings.warn(err)
            return False
        return True
    else:
        return True

# Lookups
schema_cache = {}
schemas_list = {}
repo_keys = {}
priority = []
model_names = []
type_hierarchy = {
    "null": 0,
    "boolean": 1,
    "integer": 2,
    "number": 3,
    "array": 4,
    "object": 5,
    "string": 6
}

# Hard coded default resolver and schemas to use before we have checked the resolver is valid
default_resolver = {"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1", "data": {"cacheSize": 500, "repositories": [{"name": "Iglu Central", "priority": 0, "vendorPrefixes": [ "com.snowplowanalytics" ], "connection": {"http": {"uri": "http://iglucentral.com"}}}]}}
resolver_schema = {"description":"Schema for an Iglu resolver\'s configuration","properties":{"cacheSize":{"type":"number"},"cacheTtl":{"type":["integer","null"],"minimum":0},"repositories":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"priority":{"type":"number"},"vendorPrefixes":{"type":"array","items":{"type":"string"}},"connection":{"type":"object","oneOf":[{"properties":{"embedded":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":False}},"required":["embedded"],"additionalProperties":False},{"properties":{"http":{"type":"object","properties":{"uri":{"type":"string","format":"uri"},"apikey":{"type":["string","null"]}},"required":["uri"],"additionalProperties":False}},"required":["http"],"additionalProperties":False}]}},"required":["name","priority","vendorPrefixes","connection"],"additionalProperties":False}}},"additionalProperties":False,"type":"object","required":["cacheSize","repositories"],"self":{"vendor":"com.snowplowanalytics.iglu","name":"resolver-config","format":"jsonschema","version":"1-0-3"},"$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#"}
config_schema = { "description": "Schema for an event split python script configuration", "properties": { "config": { "type": "object", "properties": { "resolver_file_path": { "type": "string" }, "filtered_events_table_name": { "type": "string" }, "users_table_name": { "type": "string" }, "validate_schemas": { "type": "boolean" }, "overwrite": { "type": "boolean" }, "models_folder": { "type": "string" } }, "required": [ "resolver_file_path" ], "additionalProperties": False }, "events": { "type": "array", "items": { "type": "object", "properties": { "event_name": { "type": "string" }, "event_columns": { "type": "array", "items": { "type": "string" } }, "self_describing_event_schema": { "type": "string" }, "context_schemas": { "type": "array", "items": { "type": "string" } }, "context_aliases": { "type": "array", "items": { "type": "string" } }, "table_name": { "type": "string" } }, "anyOf": [ { "required": [ "event_name", "self_describing_event_schema" ] }, { "required": [ "event_name", "context_schemas" ] }, { "required": [ "event_name", "event_columns" ] } ], "additionalProperties": False }, "minItems": 1 }, "users": { "type": "array", "items": { "type": "string" } } }, "additionalProperties": False, "type": "object", "required": [ "config", "events" ], "self": { "name": "splitter-config", "format": "jsonschema", "version": "1-0-3" } }

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
    raise ValueError('Invalid config file format.')

# Parse config values
user_urls = config.get('users')
event_names = []
sde_urls = []
context_urls = []
flat_cols = []
context_aliases = []
table_names = []
for event in config.get('events'):
    # Importantly, append None if it isn't provided
    event_names.append(event.get('event_name'))
    sde_urls.append(event.get('self_describing_event_schema'))
    context_urls.append(event.get('context_schemas'))
    flat_cols.append(event.get('event_columns'))
    context_aliases.append(event.get('context_aliases'))
    table_names.append(event.get('table_name'))


# Set defaults if they don't exist
validate_schemas = config.get('config').get('overwrite') or True
overwrite = config.get('config').get('overwrite') or True
resolver_file_path = config.get('config').get('resolver_file_path')
models_folder = config.get('config').get('models_folder') or 'snowplow_split_events'
filtered_events_table_name = config.get('config').get('filtered_events_table_name') or None
user_table_name = config.get('config').get('users_table_name') or 'events_users'

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
        repo_schemas = get_schema(parsed_uri.scheme + '://'+ repo_netloc + '/schemas')
    else:
        repo_schemas = get_schema(parsed_uri.scheme + '://'+ repo_netloc + '/api/schemas')
    schemas_list[repo_uri] = repo_schemas

schemas_list = {x[0]: x[1] for _, x in sorted(zip(priority, schemas_list.items()))}


######################
# Produce each model #
######################
for i in range(len(event_names)):
    event_name = event_names[i]
    verboseprint(f'Generating model {event_name}')
    sde_url = sde_urls[i]
    context_url = context_urls[i]
    flat_col = flat_cols[i]
    context_alias = context_aliases[i]
    table_name = table_names[i]

    if sde_url is not None:
        # Parse the input URL then get parse and validate schemas for sde
        sde_url_cut = urlparse(sde_url).path
        sde_json = get_schema(parse_schema_url(sde_url))
        if not validate_json(sde_json, validate = validate_schemas):
            raise ValueError(f'Validation of schema {sde_url} failed.')
        sde_major_version = sde_url.split('-')[0][-1]
        # Generate final form data for insert into model
        sde_col = 'UNSTRUCT_EVENT_' + url_to_column(sde_url_cut)
        sde_keys = list(sde_json.get('properties').keys())
        sde_types = get_types(sde_json)
    else:
        sde_major_version = '1' #Need this for all for table name
        sde_col = None
        sde_keys = None
        sde_types = None

    if context_url is not None:
        # Parse the input URL then get parse and validate schemas for contexts
        context_url_cut = [urlparse(url).path for url in context_url]
        context_jsons = [get_schema(parse_schema_url(url)) for url in context_url]
        for i, context_json in enumerate(context_jsons):
            if not validate_json(context_json, validate = validate_schemas):
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
    tags = "snowplow_web_incremental",
    materialized = var("snowplow__incremental_materialization"),
    unique_key = "event_id",
    upsert_date_key = "collector_tstamp"
) }}}}

{{%- set event_name = "{event_name}" -%}}
{{%- set flat_cols = {flat_col or []} -%}}
{{%- set sde_col = "{sde_col or "''"}" -%}}
{{%- set sde_keys = {sde_keys or []} -%}}
{{%- set sde_types = {sde_types or []} -%}}
{{%- set context_cols = {context_cols or []} -%}}
{{%- set context_keys = {context_keys or []} -%}}
{{%- set context_types = {context_types or []} -%}}
{{%- set context_alias = {context_alias or []} -%}}

{{{{ split_events(
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
    model_name = event_name + '_' + sde_major_version if table_name is None else table_name
    model_names.append(model_name)
    filename = f'./models/{models_folder}/' + model_name + '.sql'
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
    filtered_model_content = """{{ config(
    tags = "snowplow_web_incremental",
    materialized = var("snowplow__incremental_materialization"),
    unique_key = "event_id",
    upsert_date_key = "collector_tstamp"
) }}
"""

    for n, (model, event_name) in enumerate(zip(model_names, event_names)):

        filtered_model_content += f"""
select
    event_id
    , collector_tstamp
    , {event_name}
    , {model} as event_table_name
from 
    {{{{ ref('snowplow_web_base_events_this_run') }}}}
where 
    event_name = {event_name}
    and {{{{ snowplow_utils.is_run_with_new_events("snowplow_web") }}}}
        """
        if n != n_models -1:
            filtered_model_content += """
UNION ALL
"""

    filename = f'./models/{models_folder}/' + filtered_events_table_name + '.sql'
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
    user_jsons = [get_schema(parse_schema_url(url)) for url in user_urls]
    for i, user_json in enumerate(user_jsons):
        if not validate_json(user_json, validate = validate_schemas):
            raise ValueError(f'Validation of schema {user_urls[i]} failed.')
    # Generate final form data for insert into model
    user_cols = ['CONTEXTS_' + url_to_column(url) for url in user_url_cut]
    user_keys = [list(user.get('properties').keys()) for user in user_jsons]
    user_types = [get_types(user) for user in user_jsons]

    users_model_content = f"""{{{{ config(
    tags = "snowplow_web_incremental",
    materialized = var("snowplow__incremental_materialization"),
    unique_key = "user_id",
    upsert_date_key = "latest_collector_tstamp"
) }}}}

{{%- set user_cols = {user_cols or []} -%}}
{{%- set user_keys = {user_keys or []} -%}}
{{%- set user_types = {user_types or []} -%}}

{{{{ users_table(
    user_cols,
    user_keys,
    user_types
) }}}}
"""

    filename = f'./models/{models_folder}/' + user_table_name + '.sql'
    verboseprint(f'Model content for {user_table_name}, saving to {filename}:')
    verboseprint(users_model_content)
    if not args.dryRun:
        write_model_file(filename, users_model_content, overwrite = overwrite)
else:
    verboseprint('No users events table model to generate...')


verboseprint('Finished!')
