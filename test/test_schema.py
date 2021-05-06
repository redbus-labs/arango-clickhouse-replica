from copy import deepcopy

import pytest

from replication.consumer.transformer import convert_to_ch_dict_using_schema

schema = {
    "properties": {
        "Id": {
            "type": "int",
            "ch_type": "Int64",
            "ref": "_key"
        },
        "Name": {
            "type": "str",
            "ref": "name",
            "default": "temp"
        },
        "Attr1": {
            "type": "int",
            "ref": "attr1",
            "default": 10
        },
        "Attr2": {
            "type": "int",
            "ref": "attr2",
            "required": True
        }
    },
    "primary_key": "Id"
}


@pytest.mark.parametrize('doc, expected', [
    # one field casting
    ({'_key': '1', 'name': 't1', 'attr1': 1, 'attr2': 2},
     {'Id': 1, 'Name': 't1', 'Attr1': 1, 'Attr2': 2}),

    # two fields casting
    ({'_key': '1', 'name': 't1', 'attr1': '1', 'attr2': 2},
     {'Id': 1, 'Name': 't1', 'Attr1': 1, 'Attr2': 2}),

    # default assignment
    ({'_key': '1', 'name': 't1', 'attr2': 2},
     {'Id': 1, 'Name': 't1', 'Attr1': 10, 'Attr2': 2}),
])
def test_data_type_casting(doc, expected):
    result = convert_to_ch_dict_using_schema(schema, doc)
    assert result == expected


@pytest.mark.parametrize('doc, expected', [
    # primary key value not present
    ({'name': 't1', 'attr1': 1, 'attr2': 2},
     ValueError),

    # required parameter value is missing
    ({'_key': '1', 'name': 't1', 'attr1': '1'},
     ValueError)
])
def test_field_constraints(doc, expected):
    # noinspection PyBroadException
    try:
        convert_to_ch_dict_using_schema(schema, doc)
        assert False
    except Exception as e:
        print(e)
        assert isinstance(e, expected)


@pytest.mark.parametrize('doc, expected', [
    # custom mapping is not defined
    ({'_key': '1', 'name': 't1', 'attr1': 1, 'attr2': 2},
     AttributeError),
])
def test_custom_mapping(doc, expected):
    # noinspection PyBroadException
    try:
        schema_new = deepcopy(schema)
        schema_new['properties']['Name']['type'] = 'str1'
        convert_to_ch_dict_using_schema(schema_new, doc)
        assert False
    except Exception as e:
        print(e)
        assert isinstance(e, expected)
