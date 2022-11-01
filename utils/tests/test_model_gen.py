import pytest
from utils.functions.snowplow_model_gen_funcs import *

@pytest.mark.parametrize("test_input,expected", [
    ("com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1", "COM_SNOWPLOWANALYTICS_SNOWPLOW_LINK_CLICK_1_0_1"),
    ("COM.SNOWPLOWANALYTICS.SNOWPLOW/JSONSCHEMA/JSONSCHEMA/1-0-0", "COM_SNOWPLOWANALYTICS_SNOWPLOW_JSONSCHEMA_1_0_0"), 
    ("com.google.tag-manager.server-side/exception/jsonschema/2-8-1", "COM_GOOGLE_TAG_MANAGER_SERVER_SIDE_EXCEPTION_2_8_1")
    ])
def test_url_to_column(test_input, expected):
    assert url_to_column(test_input) == expected

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
        {'https://com-demo-private.net': ['iglu:com.demo/example_event_priv/jsonschema/1-0-0', 'iglu:com.demo2/test_event_priv/jsonschema/1-0-0'], 'http://iglucentral.com': ['iglu:com.demo/example_event_pub/jsonschema/1-0-0', 'iglu:com.demo2/test_event_pub/jsonschema/1-0-0']}, 
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
        {'https://com-demo-private.net': ['iglu:com.demo/example_event/jsonschema/1-0-0', 'iglu:com.demo/test_event/jsonschema/1-0-0'], 'http://iglucentral.com': ['iglu:com.demo/example_event/jsonschema/1-0-0', 'iglu:com.demo/test_event/jsonschema/1-0-0']}, 
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

    @pytest.mark.filterwarnings("ignore:UserWarning")
    def test_invalid_json(self):
        jsonData = {"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1", "data": {"repositories": [{"name": "Iglu Central", "priority": 0, "vendorPrefixes": [ "com.snowplowanalytics" ], "connection": {"http": {"uri": "http://iglucentral.com"}}}]}}
        schema = {"description":"Schema for an Iglu resolver\'s configuration","properties":{"cacheSize":{"type":"number"},"cacheTtl":{"type":["integer","null"],"minimum":0},"repositories":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"priority":{"type":"number"},"vendorPrefixes":{"type":"array","items":{"type":"string"}},"connection":{"type":"object","oneOf":[{"properties":{"embedded":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":False}},"required":["embedded"],"additionalProperties":False},{"properties":{"http":{"type":"object","properties":{"uri":{"type":"string","format":"uri"},"apikey":{"type":["string","null"]}},"required":["uri"],"additionalProperties":False}},"required":["http"],"additionalProperties":False}]}},"required":["name","priority","vendorPrefixes","connection"],"additionalProperties":False}}},"additionalProperties":False,"type":"object","required":["cacheSize","repositories"],"self":{"vendor":"com.snowplowanalytics.iglu","name":"resolver-config","format":"jsonschema","version":"1-0-3"}}
        assert not validate_json(jsonData.get('data'), schema)
    
    def test_warning(self):
        with pytest.warns(UserWarning):
            jsonData = {"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1", "data": {"repositories": [{"name": "Iglu Central", "priority": 0, "vendorPrefixes": [ "com.snowplowanalytics" ], "connection": {"http": {"uri": "http://iglucentral.com"}}}]}}
            schema = {"description":"Schema for an Iglu resolver\'s configuration","properties":{"cacheSize":{"type":"number"},"cacheTtl":{"type":["integer","null"],"minimum":0},"repositories":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"priority":{"type":"number"},"vendorPrefixes":{"type":"array","items":{"type":"string"}},"connection":{"type":"object","oneOf":[{"properties":{"embedded":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":False}},"required":["embedded"],"additionalProperties":False},{"properties":{"http":{"type":"object","properties":{"uri":{"type":"string","format":"uri"},"apikey":{"type":["string","null"]}},"required":["uri"],"additionalProperties":False}},"required":["http"],"additionalProperties":False}]}},"required":["name","priority","vendorPrefixes","connection"],"additionalProperties":False}}},"additionalProperties":False,"type":"object","required":["cacheSize","repositories"],"self":{"vendor":"com.snowplowanalytics.iglu","name":"resolver-config","format":"jsonschema","version":"1-0-3"}}
            validate_json(jsonData.get('data'), schema)

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
