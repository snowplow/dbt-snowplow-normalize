import pytest
import uuid
import shutil
import re
from os import system
import string
from utils.functions.snowplow_model_gen_funcs import *

def pop2(list, i):
    list.pop(i)
    return list

def pop3(list, i):
    for j in i:
        list.pop(j)
    return list

def compare(s1, s2):
    #https://stackoverflow.com/a/69564731
    remove = string.whitespace
    mapping = {ord(c): None for c in remove}
    print(f'Mapping: \n{mapping}')
    return s1.translate(mapping) == s2.translate(mapping)

@pytest.mark.parametrize("test_input,expected", [
    ("com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1", "COM_SNOWPLOWANALYTICS_SNOWPLOW_LINK_CLICK_1_0_1"),
    ("COM.SNOWPLOWANALYTICS.SNOWPLOW/JSONSCHEMA/JSONSCHEMA/1-0-0", "COM_SNOWPLOWANALYTICS_SNOWPLOW_JSONSCHEMA_1_0_0"),
    ("com.google.tag-manager.server-side/exception/jsonschema/2-8-1", "COM_GOOGLE_TAG_MANAGER_SERVER_SIDE_EXCEPTION_2_8_1")
    ])
def test_url_to_column(test_input, expected):
    assert url_to_column(test_input) == expected

@pytest.mark.parametrize("test_input_events,test_input_urls,test_input_versions,test_input_tables,test_prefix,expected", [
    ([['event1'], ['event2'], ['event3']], [None, None, None], [None, '5', '9'], [None, None, None], 'itsaprefix', ['itsaprefix_event1_1', 'itsaprefix_event2_5', 'itsaprefix_event3_9']),
    ([['event1'], ['event2'], ['event3']], [['iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1'], ['iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0'], ['iglu:com.snowplowanalytics.snowplow/site_search/jsonschema/1-0-0']], ['5', '2', '9'], [None, None, None], 'itsaprefix', ['itsaprefix_event1_1', 'itsaprefix_event2_1', 'itsaprefix_event3_1']),
    ([['event1'], ['event2'], ['event3']], [['iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1'], None, ['iglu:com.snowplowanalytics.snowplow/site_search/jsonschema/1-0-0']], ['5', '2', '9'], ['name1', 'name2', 'name3'], 'itsaprefix', ['name1_1', 'name2_2', 'name3_1']),
    ([['event1', 'event4'], ['event2'], ['event3']], [['iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1', 'iglu:com.snowplowanalytics.snowplow/deeplink_click/jsonschema/1-0-4'], None, ['iglu:com.snowplowanalytics.snowplow/site_search/jsonschema/1-0-0']], ['5', '2', '9'], ['name1', 'name2', 'name3'], 'itsaprefix', ['name1_5', 'name2_2', 'name3_1'])
    ])
def test_generate_names(test_input_events, test_input_urls, test_input_versions, test_input_tables, test_prefix, expected):
    assert generate_names(test_input_events, test_input_urls, test_input_versions, test_input_tables, test_prefix) == expected



def test_duplicates(capfd):
    system(f'python {os.path.join("utils", "snowplow_normalize_model_gen.py")} {os.path.join("utils", "tests", "test_normalize_config_duplicate.json")}')
    out, err = capfd.readouterr()
    assert re.match(r"^KeyError: \"Configruation leads to duplicate event names, please remove the duplicates and try again\. Duplicates: \['snowplow_event_name1_1'\]\"$", err.split('\n')[-2])

def test_missing_api(capfd):
    system(f'python {os.path.join("utils", "snowplow_normalize_model_gen.py")} {os.path.join("utils", "tests", "test_normalize_config_invalid_resolver.json")}')
    out, err = capfd.readouterr()
    assert re.match(r"^KeyError: 'A private registry uri should end in \"/api\", https://normalize-test-prod\.iglu\.snplow\.net does not, see https://docs\.snowplow\.io/docs/pipeline-components-and-applications/iglu/iglu-resolver/ for more details\.'$", err.split('\n')[-2])

# Test when there is a clash with the user id
class Test_clashing_user_id:
    @pytest.fixture(scope='class') # Only run once per class
    def setup_teardown(self):
        model_folder = str(uuid.uuid4())

        with open(os.path.join("utils", "tests", "test_normalize_config_clash_user.json"), 'r') as file:
            config_template = file.read()
        config_template = config_template.replace('$1', model_folder)

        with open(os.path.join("utils", "tests", "test_normalize_config_clash_user_filled.json"), 'w') as file:
            file.write(config_template)

        yield model_folder

        # teardown code
        shutil.rmtree(os.path.join('models', model_folder))
        os.remove(os.path.join("utils", "tests", "test_normalize_config_clash_user_filled.json"))

    def test_clashing_user_id_key(self, setup_teardown, capfd):
        system(f'python {os.path.join("utils", "snowplow_normalize_model_gen.py")} {os.path.join("utils", "tests", "test_normalize_config_clash_user_filled.json")}')
        out, err = capfd.readouterr()
        assert re.match(r"^KeyError: 'The user id alias \(spider_or_robot\) exists as a key in one of your contexts \(once converted to snakecase\), please provide an alternative user id alias in the users section of your config\.'$", err.split('\n')[-2])


class Test_types:
    def test_get_types(self):
        input = {'properties': {
            'col1': {'type': ['null', 'number']}, # 2 types including null
            'col2': {'type': "number"}, # single type
            'col3': {'type': "ARRAY"}, #capitilisation
            'col4': {'type': ['boolean', 'string', 'object']}, # 3 types
            'col5': {'enum': ['1', '4', '5']}, # enum with all integers
            'col6': {'enum': ['999', '-4', '5.7']}, # enum with mixed numbers
            'col7': {'enum': ['random', '2', 'words']}, # enum with mixed types,
            'col8': {'type': 'null'} ,# somehow a null type column
            'col9': {'type': ['integer', 'integer']} # duplicate values
            }
        }
        output = ['number', 'number', 'array', 'string', 'number', 'number', 'string', 'boolean', 'integer']

        assert get_types(input) == output

    def test_get_types_raises(self):
        with pytest.raises(ValueError):
            get_types({'properties': {'col': {'typo': ['null', 'Number']}}})

    def test_type_hierarchy(self):
        assert sorted(type_hierarchy.keys(), key = lambda x: type_hierarchy[x]) == ['null', 'boolean', 'integer', 'number', 'array', 'object', 'string']

class Test_parse_args:
    def test_verbose(self):
        args = parse_args(['-v', 'config_path'])
        args2 = parse_args(['--verbose', 'config_path'])
        args3 = parse_args(['config_path'])
        assert args.verbose and args2.verbose and not args3.verbose

    def test_help(self):
        with pytest.raises(SystemExit):
            parse_args(['-h'])

    def test_version(self):
        with pytest.raises(SystemExit):
            parse_args(['-version'])

    def test_dryRun(self):
        args = parse_args(['--dryRun', 'config_path'])
        args2 = parse_args(['config_path'])
        assert args.dryRun and not args2.dryRun

    def test_req_config(self):
        with pytest.raises(SystemExit):
            parse_args(['--dryRun'])

    def test_configHelp_exit(self):
        with pytest.raises(SystemExit):
            parse_args(['--configHelp'])

    def test_cleanUp(self):
        args = parse_args(['--cleanUp', 'config_path'])
        args2 = parse_args(['config_path'])
        assert args.cleanUp and not args2.cleanUp

class Test_write_model_file:
    def test_basic_write(self, tmpdir):
        file = tmpdir.join('output.txt')
        write_model_file(file.strpath, 'Hello\n World!')
        assert file.read() == 'Hello\n World!'

    def test_no_overwrite(self, tmpdir):
        file = tmpdir.join('dont_overwrite.txt')
        file.write("Please don't overwrite me.")
        write_model_file(file.strpath, 'Hello\n World!', False)
        assert file.read() == "Please don't overwrite me."

    def test_overwrite(self, tmpdir):
        file = tmpdir.join('overwrite.txt')
        file.write("Please do overwrite me.")
        write_model_file(file.strpath, 'Hello\n World!', True)
        assert file.read() == 'Hello\n World!'

class Test_parse_schema_url:
    def test_public_url(self):
        parsed_url = parse_schema_url('iglu:com.demo/example_event_pub/jsonschema/1-0-0',
        {'https://com-demo-private.net': ['iglu:com.demo/example_event_priv/jsonschema/1-0-0', 'iglu:com.demo2/test_event_priv/jsonschema/1-0-0'], 'http://iglucentral.com': ['iglu:com.demo/example_event_pub/jsonschema/1-0-0', 'iglu:com.demo2/test_event_pub/jsonschema/1-0-0']},
        {'iglucentral.com': None, 'com-demo-private.net': 'demo-key'}
        )
        assert parsed_url == 'http://iglucentral.com/schemas/com.demo/example_event_pub/jsonschema/1-0-0'

    def test_private_url(self):
        parsed_url = parse_schema_url('iglu:com.demo2/test_event_priv/jsonschema/1-0-0',
        {'https://com-demo-private.net/api': ['iglu:com.demo/example_event_priv/jsonschema/1-0-0', 'iglu:com.demo2/test_event_priv/jsonschema/1-0-0'], 'http://iglucentral.com': ['iglu:com.demo/example_event_pub/jsonschema/1-0-0', 'iglu:com.demo2/test_event_pub/jsonschema/1-0-0']},
        {'iglucentral.com': None, 'com-demo-private.net': 'demo-key'})
        assert parsed_url == 'https://com-demo-private.net/api/schemas/com.demo2/test_event_priv/jsonschema/1-0-0'

    def test_http_url(self):
        parsed_url = parse_schema_url('http://does-not-matter.com/what/the/url/is',
        {'https://com-demo-private.net': ['iglu:com.demo/example_event_priv/jsonschema/1-0-0', 'iglu:com.demo2/test_event_priv/jsonschema/1-0-0'], 'http://iglucentral.com': ['iglu:com.demo/example_event_pub/jsonschema/1-0-0', 'iglu:com.demo2/test_event_pub/jsonschema/1-0-0']},
        {'iglucentral.com': None, 'com-demo-private.net': 'demo-key'})
        assert parsed_url == 'http://does-not-matter.com/what/the/url/is'

    def test_not_found(self):
        with pytest.raises(ValueError):
            parsed_url = parse_schema_url('iglu:com.demo2/extra_event/jsonschema/1-0-0',
            {'https://com-demo-private.net': ['iglu:com.demo/example_event_priv/jsonschema/1-0-0', 'iglu:com.demo2/test_event_priv/jsonschema/1-0-0'], 'http://iglucentral.com': ['iglu:com.demo/example_event_pub/jsonschema/1-0-0', 'iglu:com.demo2/test_event_pub/jsonschema/1-0-0']},
            {'iglucentral.com': None, 'com-demo-private.net': 'demo-key'})

    def test_priority(self):
        parsed_url = parse_schema_url('iglu:com.demo/test_event/jsonschema/1-0-0',
        {'https://com-demo-private.net/api': ['iglu:com.demo/example_event/jsonschema/1-0-0', 'iglu:com.demo/test_event/jsonschema/1-0-0'], 'http://iglucentral.com': ['iglu:com.demo/example_event/jsonschema/1-0-0', 'iglu:com.demo/test_event/jsonschema/1-0-0']},
        {'iglucentral.com': None, 'com-demo-private.net': 'demo-key'})
        assert parsed_url == 'https://com-demo-private.net/api/schemas/com.demo/test_event/jsonschema/1-0-0'

    def test_unexpcted_schema(self):
        with pytest.raises(ValueError):
            parse_schema_url('pingu:com.snowplow.test', {}, {})

class Test_validate_json:
    def test_validate_flag(self):
        assert validate_json(dict(), dict(), False)

    def test_valid_json(self):
        jsonData = {"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1", "data": {"cacheSize": 500, "repositories": [{"name": "Iglu Central", "priority": 0, "vendorPrefixes": [ "com.snowplowanalytics" ], "connection": {"http": {"uri": "http://iglucentral.com"}}}]}}
        schema = {"description":"Schema for an Iglu resolver\'s configuration","properties":{"cacheSize":{"type":"number"},"cacheTtl":{"type":["integer","null"],"minimum":0},"repositories":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"priority":{"type":"number"},"vendorPrefixes":{"type":"array","items":{"type":"string"}},"connection":{"type":"object","oneOf":[{"properties":{"embedded":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":False}},"required":["embedded"],"additionalProperties":False},{"properties":{"http":{"type":"object","properties":{"uri":{"type":"string","format":"uri"},"apikey":{"type":["string","null"]}},"required":["uri"],"additionalProperties":False}},"required":["http"],"additionalProperties":False}]}},"required":["name","priority","vendorPrefixes","connection"],"additionalProperties":False}}},"additionalProperties":False,"type":"object","required":["cacheSize","repositories"],"self":{"vendor":"com.snowplowanalytics.iglu","name":"resolver-config","format":"jsonschema","version":"1-0-3"}}
        assert validate_json(jsonData.get('data'), schema)

    @pytest.mark.filterwarnings("ignore::UserWarning")
    def test_invalid_json(self):
        jsonData = {"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1", "data": {"repositories": [{"name": "Iglu Central", "priority": 0, "vendorPrefixes": [ "com.snowplowanalytics" ], "connection": {"http": {"uri": "http://iglucentral.com"}}}]}}
        schema = {"description":"Schema for an Iglu resolver\'s configuration","properties":{"cacheSize":{"type":"number"},"cacheTtl":{"type":["integer","null"],"minimum":0},"repositories":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"priority":{"type":"number"},"vendorPrefixes":{"type":"array","items":{"type":"string"}},"connection":{"type":"object","oneOf":[{"properties":{"embedded":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":False}},"required":["embedded"],"additionalProperties":False},{"properties":{"http":{"type":"object","properties":{"uri":{"type":"string","format":"uri"},"apikey":{"type":["string","null"]}},"required":["uri"],"additionalProperties":False}},"required":["http"],"additionalProperties":False}]}},"required":["name","priority","vendorPrefixes","connection"],"additionalProperties":False}}},"additionalProperties":False,"type":"object","required":["cacheSize","repositories"],"self":{"vendor":"com.snowplowanalytics.iglu","name":"resolver-config","format":"jsonschema","version":"1-0-3"}}
        assert not validate_json(jsonData.get('data'), schema)

    def test_warning(self):
        with pytest.warns(UserWarning):
            jsonData = {"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1", "data": {"repositories": [{"name": "Iglu Central", "priority": 0, "vendorPrefixes": [ "com.snowplowanalytics" ], "connection": {"http": {"uri": "http://iglucentral.com"}}}]}}
            schema = {"description":"Schema for an Iglu resolver\'s configuration","properties":{"cacheSize":{"type":"number"},"cacheTtl":{"type":["integer","null"],"minimum":0},"repositories":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"priority":{"type":"number"},"vendorPrefixes":{"type":"array","items":{"type":"string"}},"connection":{"type":"object","oneOf":[{"properties":{"embedded":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":False}},"required":["embedded"],"additionalProperties":False},{"properties":{"http":{"type":"object","properties":{"uri":{"type":"string","format":"uri"},"apikey":{"type":["string","null"]}},"required":["uri"],"additionalProperties":False}},"required":["http"],"additionalProperties":False}]}},"required":["name","priority","vendorPrefixes","connection"],"additionalProperties":False}}},"additionalProperties":False,"type":"object","required":["cacheSize","repositories"],"self":{"vendor":"com.snowplowanalytics.iglu","name":"resolver-config","format":"jsonschema","version":"1-0-3"}}
            validate_json(jsonData.get('data'), schema)

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_get_schema(self):
        jsonData = {"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1", "data": {"cacheSize": 500, "repositories": [{"name": "Iglu Central", "priority": 0, "vendorPrefixes": [ "com.snowplowanalytics" ], "connection": {"http": {"uri": "http://iglucentral.com"}}}]}}
        assert validate_json(jsonData,
                            schemas_list = { 'http://iglucentral.com': ['iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1']},
                            repo_keys = {'iglucentral.com': None})

    def test_no_input_error(self):
        with pytest.raises(ValueError):
            jsonData = {"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1", "data": {"cacheSize": 500, "repositories": [{"name": "Iglu Central", "priority": 0, "vendorPrefixes": [ "com.snowplowanalytics" ], "connection": {"http": {"uri": "http://iglucentral.com"}}}]}}
            validate_json(jsonData)

    def test_no_schema_error(self):
        with pytest.raises(ValueError):
            jsonData = {"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1", "data": {"cacheSize": 500, "repositories": [{"name": "Iglu Central", "priority": 0, "vendorPrefixes": [ "com.snowplowanalytics" ], "connection": {"http": {"uri": "http://iglucentral.com"}}}]}}
            validate_json(jsonData.get('data'),
                            schemas_list = { 'http://iglucentral.com': ['iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1']},
                            repo_keys = {'iglucentral.com': None})

class Test_get_schema:
    def test_public_repo(self):
        got_schema = get_schema('http://iglucentral.com/schemas/com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1', {})
        expected = {"description":"Schema for a link click event","properties":{"elementId":{"type":"string"},"elementClasses":{"type":"array","items":{"type":"string"}},"elementTarget":{"type":"string"},"targetUrl":{"type":"string","minLength":1},"elementContent":{"type":"string"}},"additionalProperties":False,"type":"object","required":["targetUrl"],"self":{"vendor":"com.snowplowanalytics.snowplow","name":"link_click","format":"jsonschema","version":"1-0-1"},"$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#"}
        assert got_schema == expected

    def test_use_cache(self):
        got_schema = get_schema('http://iglucentral.com/schemas/com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1', {})
        got_schema2 = get_schema('http://iglucentral.com/schemas/com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1', {})
        assert got_schema == got_schema2

class Test_cleanup_models:
    @pytest.fixture(scope='function')
    def setup_teardown(self):
        # Due to the cleanup function accessing the models folder we need
        # to actually test this on files in that folder so can't use a temp
        # directory to do the testing

        # Generate folder and file names - use uuids so it's obvious to
        # delete them if anything goes wrong and they are left over
        keep_folder = str(uuid.uuid4())
        model_folder = str(uuid.uuid4())
        # Make directories
        os.makedirs(os.path.join('models', keep_folder))
        os.makedirs(os.path.join('models', model_folder))

        # Generate a list of fake model file names, first numbers are file/generated numbers, second is input ranges
        # Ordering is not ideal given the misalignment between lists, but it works.
        # 0-4 (.-.): user models in different folder, don't delete
        # 5-9 (0-4): table_name provided models
        # 10-14 (5-9): event_name models, with sdes
        # 15-20 (10-14): event_name models, no sde, version provided
        # 20-24 (15-20): event name models, no sde, no version provided
        # 25-40 (20-35): extra models in config that don't exist (not in filename list)
        # 40 (.) user table
        # 41 (.) filtered table
        generated_names = [str(uuid.uuid4()) for i in range(42)]
        sde_urls_fixed = ["iglu:com.snowplowanalytics.snowplow/test/jsonschema/9-0-0",
                    "iglu:com.snowplowanalytics.snowplow/test/jsonschema/2-0-0",
                    "iglu:com.snowplowanalytics.snowplow/test/jsonschema/9-5-9",
                    "iglu:com.snowplowanalytics.snowplow/test/jsonschema/3-0-0",
                    "iglu:com.snowplowanalytics.snowplow/test/jsonschema/5-4-3"]
        sde_url_versions = ['9', '2', '9', '3', '5']
        versions_fixed = ['6', '2', '9', '8', '0']
        users_table = generated_names[40]
        filtered_table = generated_names[41]
        event_names = []
        sde_urls = []
        versions = []
        table_names = []
        file_names = []
        for i, name in enumerate(generated_names):
            # user models in different folder, don't delete
            if i >= 0 and i < 5:
                filename = os.path.join('models', keep_folder, name + '.sql')
                file_names.append(filename)
                with open(filename, 'w'):
                    pass

            # table_name provided models
            elif i >= 5 and i < 10:
                event_names.append(['Does not matter ' + str(i)])
                sde_urls.append(None)
                versions.append(None)
                table_names.append(name)
                filename = os.path.join('models', model_folder, name + '_1.sql')
                file_names.append(filename)
                with open(filename, 'w'):
                    pass

            # event_name models, with sdes
            elif i >= 10 and i < 15:
                event_names.append([name])
                sde_urls.append([sde_urls_fixed[i - 10]])
                versions.append(sde_url_versions[i - 10])
                table_names.append(None)
                filename = os.path.join('models', model_folder, name + '_' + sde_url_versions[i - 10] + '.sql')
                file_names.append(filename)
                with open(filename, 'w'):
                    pass

            # event_name models, no sde, version provided
            elif i >= 15 and i < 20:
                event_names.append([name])
                sde_urls.append(None)
                versions.append(versions_fixed[i - 15])
                table_names.append(None)
                filename = os.path.join('models', model_folder, name + '_' + versions_fixed[i - 15] + '.sql')
                file_names.append(filename)
                with open(filename, 'w'):
                    pass

            # event name models, no sde, no version provided
            elif i >= 20 and i < 25:
                event_names.append([name])
                sde_urls.append(None)
                versions.append(None)
                table_names.append(None)
                filename = os.path.join('models', model_folder, name + '_1.sql')
                file_names.append(filename)
                with open(filename, 'w'):
                    pass

            # extra models in config that don't exist on sysmte
            elif i >= 25 and i < 40:
                event_names.append(['Does not matter' + str(i)])
                sde_urls.append(None)
                versions.append(None)
                table_names.append(name)

        # write the user and filtered table files
        filename = os.path.join('models', model_folder, users_table + '.sql')
        file_names.append(filename)
        with open(filename, 'w'):
            pass
        filename = os.path.join('models', model_folder, filtered_table + '.sql')
        file_names.append(filename)
        with open(filename, 'w'):
            pass

        to_pass = {'keep_folder': keep_folder,
            'model_folder': model_folder,
            'event_names': event_names,
            'sde_urls': sde_urls,
            'versions': versions,
            'table_names': table_names,
            'users_table': users_table,
            "filtered_table":filtered_table,
            "file_names": file_names
            }
        yield to_pass

        # teardown code
        shutil.rmtree(os.path.join('models', keep_folder))
        shutil.rmtree(os.path.join('models', model_folder))

    # Keep all files in faked config input, expect none to delete
    def test_none_to_del(self, setup_teardown, capfd):
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cleanup_models(
                event_names = setup_teardown.get('event_names'),
                sde_urls = setup_teardown.get('sde_urls'),
                versions = setup_teardown.get('versions'),
                table_names= setup_teardown.get('table_names'),
                models_folder= setup_teardown.get('model_folder'),
                user_table_name= setup_teardown.get('users_table'),
                filtered_events_table_name= setup_teardown.get('filtered_table'),
                models_prefix = '',
                dry_run= False
            )
        out, err = capfd.readouterr()
        k_files = os.listdir(os.path.join('models', setup_teardown.get('keep_folder')))
        k_files = [os.path.join('models', setup_teardown.get('keep_folder'), file) for file in k_files]
        m_files = os.listdir(os.path.join('models', setup_teardown.get('model_folder')))
        m_files = [os.path.join('models', setup_teardown.get('model_folder'), file) for file in m_files]
        files = k_files + m_files
        expected_files = setup_teardown.get('file_names')
        assert out == 'No models to clean up, quitting...\n'
        assert set(files) == set(expected_files)

    # remove one file (named table), decline, expect all files remain
    def test_decline_del_n(self, setup_teardown, monkeypatch, capfd):
        # monkeypatch the "input" function, so that it returns "n".
        # This simulates the user entering "n" in the terminal:
        monkeypatch.setattr('builtins.input', lambda _: "n")
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cleanup_models(
                event_names = pop2(setup_teardown.get('event_names'), 0),
                sde_urls = pop2(setup_teardown.get('sde_urls'), 0),
                versions = pop2(setup_teardown.get('versions'), 0),
                table_names= pop2(setup_teardown.get('table_names'), 0),
                models_folder= setup_teardown.get('model_folder'),
                user_table_name= setup_teardown.get('users_table'),
                filtered_events_table_name= setup_teardown.get('filtered_table'),
                models_prefix = '',
                dry_run= False
            )
        out, err = capfd.readouterr()
        k_files = os.listdir(os.path.join('models', setup_teardown.get('keep_folder')))
        k_files = [os.path.join('models', setup_teardown.get('keep_folder'), file) for file in k_files]
        m_files = os.listdir(os.path.join('models', setup_teardown.get('model_folder')))
        m_files = [os.path.join('models', setup_teardown.get('model_folder'), file) for file in m_files]
        files = k_files + m_files
        expected_files = setup_teardown.get('file_names')
        assert re.match(r'^Cleanup will remove models: {.*}\s*$', out.split('\n')[0])
        assert re.match(r'^Models not deleted\.\s*$', out.split('\n')[1])
        assert set(files) == set(expected_files)

    # remove one file (named table), decline, expect all files remain
    def test_dry_run(self, setup_teardown, monkeypatch, capfd):
        # monkeypatch the "input" function, so that it returns "n".
        # This simulates the user entering "n" in the terminal:
        monkeypatch.setattr('builtins.input', lambda _: "n")
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cleanup_models(
                event_names = pop3(setup_teardown.get('event_names'), [0, 4, 8, 12]),
                sde_urls = pop3(setup_teardown.get('sde_urls'), [0, 4, 8, 12]),
                versions = pop3(setup_teardown.get('versions'), [0, 4, 8, 12]),
                table_names= pop3(setup_teardown.get('table_names'), [0, 4, 8, 12]),
                models_folder= setup_teardown.get('model_folder'),
                user_table_name= setup_teardown.get('users_table'),
                filtered_events_table_name= setup_teardown.get('filtered_table'),
                models_prefix = '',
                dry_run= False
            )
        out, err = capfd.readouterr()
        k_files = os.listdir(os.path.join('models', setup_teardown.get('keep_folder')))
        k_files = [os.path.join('models', setup_teardown.get('keep_folder'), file) for file in k_files]
        m_files = os.listdir(os.path.join('models', setup_teardown.get('model_folder')))
        m_files = [os.path.join('models', setup_teardown.get('model_folder'), file) for file in m_files]
        files = k_files + m_files
        expected_files = setup_teardown.get('file_names')
        assert re.match(r'^Cleanup will remove models: {.*}\s*$', out.split('\n')[0])
        assert re.match(r'^Models not deleted\.\s*$', out.split('\n')[1])
        assert set(files) == set(expected_files)

    # remove one file (named table), bad input, expect all files remain
    def test_decline_del_random(self, setup_teardown, monkeypatch, capfd):
        monkeypatch.setattr('builtins.input', lambda _: "3")
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cleanup_models(
                event_names = pop2(setup_teardown.get('event_names'), 0),
                sde_urls = pop2(setup_teardown.get('sde_urls'), 0),
                versions = pop2(setup_teardown.get('versions'), 0),
                table_names= pop2(setup_teardown.get('table_names'), 0),
                models_folder= setup_teardown.get('model_folder'),
                user_table_name= setup_teardown.get('users_table'),
                filtered_events_table_name= setup_teardown.get('filtered_table'),
                models_prefix = '',
                dry_run= False
            )
        out, err = capfd.readouterr()
        k_files = os.listdir(os.path.join('models', setup_teardown.get('keep_folder')))
        k_files = [os.path.join('models', setup_teardown.get('keep_folder'), file) for file in k_files]
        m_files = os.listdir(os.path.join('models', setup_teardown.get('model_folder')))
        m_files = [os.path.join('models', setup_teardown.get('model_folder'), file) for file in m_files]
        files = k_files + m_files
        expected_files = setup_teardown.get('file_names')
        assert re.match(r'^Cleanup will remove models: {.*}\s*$', out.split('\n')[0])
        assert re.match(r'^Models not deleted\.\s*$', out.split('\n')[1])
        assert set(files) == set(expected_files)

    # remove one file (named table), lowercase input, expect all files remain
    def test_decline_del_lowercase(self, setup_teardown, monkeypatch, capfd):
        monkeypatch.setattr('builtins.input', lambda _: "y")
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cleanup_models(
                event_names = pop2(setup_teardown.get('event_names'), 0),
                sde_urls = pop2(setup_teardown.get('sde_urls'), 0),
                versions = pop2(setup_teardown.get('versions'), 0),
                table_names= pop2(setup_teardown.get('table_names'), 0),
                models_folder= setup_teardown.get('model_folder'),
                user_table_name= setup_teardown.get('users_table'),
                filtered_events_table_name= setup_teardown.get('filtered_table'),
                models_prefix = '',
                dry_run= False
            )
        out, err = capfd.readouterr()
        k_files = os.listdir(os.path.join('models', setup_teardown.get('keep_folder')))
        k_files = [os.path.join('models', setup_teardown.get('keep_folder'), file) for file in k_files]
        m_files = os.listdir(os.path.join('models', setup_teardown.get('model_folder')))
        m_files = [os.path.join('models', setup_teardown.get('model_folder'), file) for file in m_files]
        files = k_files + m_files
        expected_files = setup_teardown.get('file_names')
        assert re.match(r'^Cleanup will remove models: {.*}\s*$', out.split('\n')[0])
        assert re.match(r'^Models not deleted\.\s*$', out.split('\n')[1])
        assert set(files) == set(expected_files)

    # remove 3 tables from config, accept, expect all other files remain
    # Delete 1 of each "type" of input, adjust the pop value as it happens in series
    def test_accept_del_some(self, setup_teardown, monkeypatch, capfd):
        monkeypatch.setattr('builtins.input', lambda _: "Y")
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cleanup_models(
                event_names = pop3(setup_teardown.get('event_names'), [0, 4, 8, 12]),
                sde_urls = pop3(setup_teardown.get('sde_urls'), [0, 4, 8, 12]),
                versions = pop3(setup_teardown.get('versions'), [0, 4, 8, 12]),
                table_names= pop3(setup_teardown.get('table_names'), [0, 4, 8, 12]),
                models_folder= setup_teardown.get('model_folder'),
                user_table_name= setup_teardown.get('users_table'),
                filtered_events_table_name= setup_teardown.get('filtered_table'),
                models_prefix = '',
                dry_run= False
            )
        out, err = capfd.readouterr()
        k_files = os.listdir(os.path.join('models', setup_teardown.get('keep_folder')))
        k_files = [os.path.join('models', setup_teardown.get('keep_folder'), file) for file in k_files]
        m_files = os.listdir(os.path.join('models', setup_teardown.get('model_folder')))
        m_files = [os.path.join('models', setup_teardown.get('model_folder'), file) for file in m_files]
        files = k_files + m_files
        expected_files = pop3(setup_teardown.get('file_names'), [0+5, 4+5, 8+5, 12+5]) # add 5 to adjust for user models
        assert re.match(r'^Cleanup will remove models: {.*}\s*$', out.split('\n')[0])
        assert re.match(r'^Deleted 4 models, quitting...\s*$', out.split('\n')[1])
        assert set(files) == set(expected_files)

    # rename filtered events table
    def test_accept_del_filtered(self, setup_teardown, monkeypatch, capfd):
        monkeypatch.setattr('builtins.input', lambda _: "Y")
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cleanup_models(
                event_names = setup_teardown.get('event_names'),
                sde_urls = setup_teardown.get('sde_urls'),
                versions = setup_teardown.get('versions'),
                table_names= setup_teardown.get('table_names'),
                models_folder= setup_teardown.get('model_folder'),
                user_table_name= setup_teardown.get('users_table'),
                filtered_events_table_name= 'dummy_name',
                models_prefix = '',
                dry_run= False
            )
        out, err = capfd.readouterr()
        k_files = os.listdir(os.path.join('models', setup_teardown.get('keep_folder')))
        k_files = [os.path.join('models', setup_teardown.get('keep_folder'), file) for file in k_files]
        m_files = os.listdir(os.path.join('models', setup_teardown.get('model_folder')))
        m_files = [os.path.join('models', setup_teardown.get('model_folder'), file) for file in m_files]
        files = k_files + m_files
        expected_files = pop2(setup_teardown.get('file_names'), -1)
        assert re.match(r'^Cleanup will remove models: {.*}\s*$', out.split('\n')[0])
        assert re.match(r'^Deleted 1 models, quitting...\s*$', out.split('\n')[1])
        assert set(files) == set(expected_files)

    # rename the events table
    def test_accept_del_users(self, setup_teardown, monkeypatch, capfd):
        monkeypatch.setattr('builtins.input', lambda _: "Y")
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cleanup_models(
                event_names = setup_teardown.get('event_names'),
                sde_urls = setup_teardown.get('sde_urls'),
                versions = setup_teardown.get('versions'),
                table_names= setup_teardown.get('table_names'),
                models_folder= setup_teardown.get('model_folder'),
                user_table_name= 'dummy_users',
                filtered_events_table_name=  setup_teardown.get('filtered_table'),
                models_prefix = '',
                dry_run= False
            )
        out, err = capfd.readouterr()
        k_files = os.listdir(os.path.join('models', setup_teardown.get('keep_folder')))
        k_files = [os.path.join('models', setup_teardown.get('keep_folder'), file) for file in k_files]
        m_files = os.listdir(os.path.join('models', setup_teardown.get('model_folder')))
        m_files = [os.path.join('models', setup_teardown.get('model_folder'), file) for file in m_files]
        files = k_files + m_files
        expected_files = pop2(setup_teardown.get('file_names'), -2)
        assert re.match(r'^Cleanup will remove models: {.*}\s*$', out.split('\n')[0])
        assert re.match(r'^Deleted 1 models, quitting...\s*$', out.split('\n')[1])
        assert set(files) == set(expected_files)

    # Just delete everything to ensure it keeps the user custom models in the otehr folder
    def test_accept_del_all(self, setup_teardown, monkeypatch, capfd):
        monkeypatch.setattr('builtins.input', lambda _: "Y")
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cleanup_models(
                event_names = [],
                sde_urls = [],
                versions = [],
                table_names= [],
                models_folder= setup_teardown.get('model_folder'),
                user_table_name= 'events_users',
                filtered_events_table_name= '',
                models_prefix = '',
                dry_run= False
            )
        out, err = capfd.readouterr()
        k_files = os.listdir(os.path.join('models', setup_teardown.get('keep_folder')))
        k_files = [os.path.join('models', setup_teardown.get('keep_folder'), file) for file in k_files]
        m_files = os.listdir(os.path.join('models', setup_teardown.get('model_folder')))
        m_files = [os.path.join('models', setup_teardown.get('model_folder'), file) for file in m_files]
        files = k_files + m_files
        expected_files = setup_teardown.get('file_names')[0:5]
        assert re.match(r'^Cleanup will remove models: {.*}\s*$', out.split('\n')[0])
        assert re.match(r'^Deleted 22 models, quitting...\s*$', out.split('\n')[1])
        assert set(files) == set(expected_files)

class Test_model_output:
    @pytest.fixture(scope='class') # Only run once per class
    def setup_teardown(self):
        model_folder = str(uuid.uuid4())

        with open(os.path.join("utils", "tests", "test_normalize_config.json"), 'r') as file:
            config_template = file.read()
        config_template = config_template.replace('$1', model_folder)

        with open(os.path.join("utils", "tests", "test_normalize_config_filled.json"), 'w') as file:
            file.write(config_template)

        system(f'python {os.path.join("utils", "snowplow_normalize_model_gen.py")} {os.path.join("utils", "tests", "test_normalize_config_filled.json")}')
        yield model_folder

        # teardown code
        shutil.rmtree(os.path.join('models', model_folder))
        os.remove(os.path.join("utils", "tests", "test_normalize_config_filled.json"))

    def test_users(self, setup_teardown):
        with open(os.path.join('models', setup_teardown, 'test_events_users.sql')) as file:
            output = file.read()

        with open(os.path.join('utils', 'tests', 'expected', 'test_events_users.sql')) as file:
            expected = file.read()

        assert compare(output, expected)

    def test_normalized(self, setup_teardown):
        with open(os.path.join('models', setup_teardown, 'test_normalized_events.sql')) as file:
            output = file.read()

        with open(os.path.join('utils', 'tests', 'expected', 'test_normalized_events.sql')) as file:
            expected = file.read()

        assert compare(output, expected)

    def test_default_name(self, setup_teardown):
        with open(os.path.join('models', setup_teardown, 'itsaprefix_event_name1_1.sql')) as file:
            output = file.read()

        with open(os.path.join('utils', 'tests', 'expected', 'event_name1_1.sql')) as file:
            expected = file.read()

        assert compare(output, expected)

    def test_no_alias(self, setup_teardown):
        with open(os.path.join('models', setup_teardown, 'custom_table_name2_1.sql')) as file:
            output = file.read()

        with open(os.path.join('utils', 'tests', 'expected', 'custom_table_name2_1.sql')) as file:
            expected = file.read()

        assert compare(output, expected)

    def test_no_sde(self, setup_teardown):
        with open(os.path.join('models', setup_teardown, 'custom_table_name3_2.sql')) as file:
            output = file.read()

        with open(os.path.join('utils', 'tests', 'expected', 'custom_table_name3_2.sql')) as file:
            expected = file.read()

        assert compare(output, expected)

    def test_full_single(self, setup_teardown):
        with open(os.path.join('models', setup_teardown, 'custom_table_name4_1.sql')) as file:
            output = file.read()

        with open(os.path.join('utils', 'tests', 'expected', 'custom_table_name4_1.sql')) as file:
            expected = file.read()

        assert compare(output, expected)

    def test_multi(self, setup_teardown):
        with open(os.path.join('models', setup_teardown, 'custom_table_name5_9.sql')) as file:
            output = file.read()

        with open(os.path.join('utils', 'tests', 'expected', 'custom_table_name5_9.sql')) as file:
            expected = file.read()

        assert compare(output, expected)

    def test_full_multi(self, setup_teardown):
        with open(os.path.join('models', setup_teardown, 'custom_table_name6_6.sql')) as file:
            output = file.read()

        with open(os.path.join('utils', 'tests', 'expected', 'custom_table_name6_6.sql')) as file:
            expected = file.read()

    def test_no_flat_cols(self, setup_teardown):
        with open(os.path.join('models', setup_teardown, 'custom_table_name7_6.sql')) as file:
            output = file.read()

        with open(os.path.join('utils', 'tests', 'expected', 'custom_table_name7_6.sql')) as file:
            expected = file.read()

        assert compare(output, expected)
