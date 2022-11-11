from typing import Union
import warnings
import jsonschema
import requests
import os
from urllib.parse import urlparse
import json
import argparse
import copy

verboseprint = lambda *a, **k: None

def write_model_file(filename: str, model_code: str, overwrite: bool = True):
    """Write model code into a file

    Note that folders will be created if they do not exist as part of the filename, and existing files will be overwritten.

    Args:
        filename (str): The name of the file to write the code to, including path
        model_code (str): String to write into the file
        overwrite (bool): Overwrite the file if it already exists. Defaults to True
    """
    if not overwrite and os.path.exists(filename):
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
            # If it is a list get the max based on the hierarchy e.g. int and str would be str
            types.append(max([cur_type.lower()] if isinstance(cur_type, str) else [type.lower() for type in cur_type], key = lambda x: type_hierarchy[x]))
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
    return [type if type != 'null' else 'boolean' for type in types] # Can't have a null type column, everything else exists in snowflake as is, not needed for other warehouses

def url_to_column(str: str) -> str:
    """convert url string to database column format

    Args:
        str (str): Input url

    Returns:
        str: Output column name cleaned of punctuation and replaced with underscores
    """
    return str.upper().replace('/JSONSCHEMA', '', 1).replace('.', '_').replace('-', '_').replace('/', '_')

def parse_schema_url(url: str, schemas_list: dict, repo_keys: dict) -> str:
    """Parse a schema URL and provide the true URL to GET request

    Args:
        url (string): the schema url to parse into a true url, should start with iglu: or http
        schemas_list (dict): A dictionary of each schema url and the list of schemas within that registry
        repo_keys (dict): A dictionary of API keys for each registry
    
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

def get_schema(url: str, repo_keys: dict) -> Union[dict, list]:
    """Return schema from url (using cache if available)

    Args:
        url (string): The URL to send a GET request to, using API key details if required
        repo_keys (dict): A dictionary of API keys for each registry

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

def validate_json(jsonData: dict, schema: dict = None, validate: bool = True, schemas_list: dict = None, repo_keys: dict = None) -> bool:
    """Validates a JSON against a schema

    Args:
        jsonData (dict): A dictionary of the JSON data of the schema to validate
        schema (dict, optional): The schema to validate against. If provided will compare otherwise will look for a "schema" property of the jsonData. Defaults to None.
        validate (bool, optional): If validation should be run or not, function returns True if no valdiation is run. Defaults to True.
        schemas_list (dict, optional): A dictionary of each schema url and the list of schemas within that registry
        repo_keys (dict, optional): A dictionary of API keys for each registry

    Returns:
        bool: If the jsonData validated succfully against the schema or not
    """
    json_copy = copy.deepcopy(jsonData)
    if validate:
        verboseprint('Validating JSON structure...')
        if schema is None: # Need to have passed a full JSON with scehma and self information
            if schemas_list is None or repo_keys is None:
                raise ValueError('No schema provided, you must provide schema_list and repo_keys in this case.')
            schema_url = json_copy.get('$schema') or json_copy.get('schema')
            if schema_url is None:
                raise ValueError(f'$schema not present in JSON and no schema provided to validate against.')
            parsed_schema = parse_schema_url(schema_url, schemas_list, repo_keys)
            schema = get_schema(parsed_schema, repo_keys)
            if json_copy.get('schema') is not None:
                json_copy = json_copy.get('data')
        try:
            jsonschema.validate(instance=json_copy, schema=schema)
        except jsonschema.exceptions.ValidationError as err:
            warnings.warn(str(err))
            return False
        return True
    else:
        return True

def cleanup_models(event_names: list, sde_urls: list, versions: list, table_names: list, models_folder: str, user_table_name: str, filtered_events_table_name: str, dry_run: bool) -> None:
    """Clean up excess models not present in your config file and quit

    Args:
        event_names (list): List of event names from your config
        sde_urls (list): List of self describing event urls from your config
        versions (list): List of versions from your config
        table_names (list): List of tables names from your config
        models_folder (string): The folder in models the script writes to from your config
        user_table_name (string): Name of your users table from your config
        filtered_events_table_name (string): Name of your filtered events table from your config
        dry_run (boolean): Do as a dry run or not
    """
    verboseprint('Starting cleanup...')
    sde_major_versions = [sde_url.split('-')[0][-1] if sde_url is not None else version if version is not None else '1' for sde_url, version in zip(sde_urls, versions)]
    model_names = [event_name + '_' + sde_major_version if table_name is None else table_name + '_' + sde_major_version for event_name, sde_major_version, table_name in zip(event_names, sde_major_versions, table_names)]
    if filtered_events_table_name is not None:
        model_names.extend([user_table_name, filtered_events_table_name])
    else:
        model_names.append(user_table_name)

    cur_models = os.listdir(os.path.join('models', models_folder))
    extra_models = set(cur_models).difference(set([model + '.sql' for model in model_names]))
    if len(extra_models) == 0:
        print('No models to clean up, quitting...')
        quit()
    print(f'Cleanup will remove models: {extra_models}')
    del_check = input('Confirm deletion of models (Y/n): ')
    if del_check == 'Y' and not dry_run:
        for model in extra_models:
            verboseprint(f'Deleting file {model}...')
            os.remove(os.path.join('models', models_folder, model))
        print(f'Deleted {len(extra_models)} models, quitting...')
        quit()
    else:
        print('Models not deleted.')
        quit()

def parse_args(args: list):
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description = 'Produce dbt model files for splitting your Snowplow events table into 1 table per event')
    parser.add_argument('config', help = 'relative path to your configuration file')
    parser.add_argument('--version', action='version',
                        version='%(prog)s V0.0.1', help="show program's version number and exit")
    parser.add_argument('-v', '--verbose', dest = 'verbose', action = 'store_true', default = False, help = 'verbose flag for the running of the tool')
    parser.add_argument('--dryRun', dest = 'dryRun', action = 'store_true', default = False, help ='flag for a dry run (does not write/delete any files)')
    parser.add_argument('--configHelp', dest = 'configHelp', action = 'version', version = config_help, help = 'prints information relating to the structure of the config file')
    parser.add_argument('--cleanUp', dest = 'cleanUp', action = 'store_true', default = False, help = 'delete any models not present in your config and exit (no models will be generated)')
    return parser.parse_args(args)

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
resolver_schema = {"description":"Schema for an Iglu resolver\'s configuration","properties":{"cacheSize":{"type":"number"},"cacheTtl":{"type":["integer","null"],"minimum":0},"repositories":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"priority":{"type":"number"},"vendorPrefixes":{"type":"array","items":{"type":"string"}},"connection":{"type":"object","oneOf":[{"properties":{"embedded":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":False}},"required":["embedded"],"additionalProperties":False},{"properties":{"http":{"type":"object","properties":{"uri":{"type":"string","format":"uri"},"apikey":{"type":["string","null"]}},"required":["uri"],"additionalProperties":False}},"required":["http"],"additionalProperties":False}]}},"required":["name","priority","vendorPrefixes","connection"],"additionalProperties":False}}},"additionalProperties":False,"type":"object","required":["cacheSize","repositories"],"self":{"vendor":"com.snowplowanalytics.iglu","name":"resolver-config","format":"jsonschema","version":"1-0-3"}}
config_schema = { "description": "Schema for the Snowplow dbt event split python script configuration", "properties": { "config": { "type": "object", "properties": { "resolver_file_path": { "type": "string" }, "filtered_events_table_name": { "type": "string" }, "users_table_name": { "type": "string" }, "validate_schemas": { "type": "boolean" }, "overwrite": { "type": "boolean" }, "models_folder": { "type": "string" } }, "required": [ "resolver_file_path" ], "additionalProperties": False }, "events": { "type": "array", "items": { "type": "object", "properties": { "event_name": { "type": "string" }, "event_columns": { "type": "array", "items": { "type": "string" } }, "self_describing_event_schema": { "type": "string" }, "context_schemas": { "type": "array", "items": { "type": "string" } }, "context_aliases": { "type": "array", "items": { "type": "string" } }, "table_name": { "type": "string" }, "version": { "type": "string", "minLength": 1, "maxLength": 1 } }, "anyOf": [ { "required": [ "event_name", "self_describing_event_schema" ] }, { "required": [ "event_name", "context_schemas" ] }, { "required": [ "event_name", "event_columns" ] } ], "additionalProperties": False }, "minItems": 1 }, "users": { "type": "array", "items": { "type": "string" } } }, "additionalProperties": False, "type": "object", "required": [ "config", "events" ], "self": { "name": "splitter-config", "format": "jsonschema", "version": "1-0-0" } }

config_help = """
JSON Config file structure:
{
    "config":{
        "resolver_file_path": <required - string: relative path to your resolver config json, or "default" to use iglucentral only>,
        "filtered_events_table_name": <optional - string: name of filtered events table, if not provided it will not be generated>,
        "users_table_name": <optional - string: name of users table, default events_users if user schema(s) provided>,
        "validate_schemas": <optional - boolean: if you want to validate schemas loaded from each iglu registry or not, default true>,
        "overwrite": <optional - boolean: overwrite existing model files or not, default true>,
        "models_folder": <optional - string: folder under models/ to place the models, default snowplow_split_events>
    },
    "events":[
        {
            "event_name": <required - string: name of the event type, value of the event_name column in your warehouse>,
            "event_columns": <optional (>=1 of) - array: array of strings of flat column names from the events table to include to include in the model>,
            "self_describing_event_schema": <optional (>=1 of) - string: `iglu:com.` type url for the self-describing event to include in the model>,
            "context_schemas": <optional (>=1 of) - array: array of strings of `iglu:com.` type url(s) for the context/entities to include in the model>,
            "context_aliases": <optional - array: array of strings of prefixes to the column alias for context/entities>,
            "table_name": <optional - string: name of the model, default is the event_name>,
            "version": <optional - string (length 1): version number to append to table name, if self_describing_event_schema is provided uses major version number from that, default 1>
        },
        {
            ...
        }
    ],
    "users": <optional - array: array of strings of iglu:com. type url(s) for the context/entities to add to your users table as columns, if not provided will not generate users model>
}"""
