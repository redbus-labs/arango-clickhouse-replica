import json
import typing
from datetime import datetime
from typing import Any, Callable, List, TypeVar

import ciso8601

try:
    from tables.transform import custom_transformers
except ImportError:
    custom_transformers = {}

T = TypeVar("T")


def from_str(x: Any) -> str:
    assert isinstance(x, str)
    return x


def from_none(x: Any) -> Any:
    assert x is None
    return x


def from_int(x: Any) -> int:
    assert isinstance(x, int) and not isinstance(x, bool)
    return x


def from_datetime(x: Any) -> datetime:
    return ciso8601.parse_datetime(x)


def from_list(f: Callable[[Any], T], x: Any) -> List[T]:
    assert isinstance(x, list)
    return [f(y) for y in x]


def is_list(x):
    return isinstance(x, list)


def decode_json(value):
    return json.loads(value)


def from_list_of_string(value):
    return from_list(str, value)


def from_list_of_int(value):
    return from_list(int, value)


def get_type_cast_function(cast_str):
    try:
        return cast_dict[cast_str]
    except KeyError:
        return None


def convert_to_ch_dict_using_schema(schema: typing.Dict, document):
    result_document = {}
    props = schema['properties']
    for key in props:
        ref_column = props[key]['ref'] if 'ref' in props[key] else key
        if ref_column in document and document[ref_column] is not None:
            value = document[ref_column]
        elif key == schema['primary_key']:
            raise ValueError(f'{key} primary key value is required')
        elif 'required' not in props[key] or not props[key]['required']:
            if 'default' in props[key]:
                value = props[key]['default']
            else:
                result_document[key] = None
                continue
        else:
            raise ValueError(f'{key} value is not present')
        type_caster = get_type_cast_function(props[key]['type'])
        if not type_caster:
            raise AttributeError(f"{props[key]['type']}: custom type cast mapping not found")
        # handle optional/multiple types for the same field
        if isinstance(type_caster, list):
            for type_cast in type_caster:
                try:
                    value = type_cast(value)
                    break
                except (AssertionError, ValueError, TypeError):
                    continue
            else:
                raise ValueError
        else:
            value = type_caster(value)
        result_document[key] = value
    return result_document


cast_dict = {
    'str': str,
    'int': int,
    'float': float,
    'bool': bool,
    'from_datetime': from_datetime,
    '[List, str]': from_list_of_string,
    '[List, int]': from_list_of_int,
    **custom_transformers
}
