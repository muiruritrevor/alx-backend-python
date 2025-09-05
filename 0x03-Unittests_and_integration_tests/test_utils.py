#!/usr/bin/env python3
# """Generic utilities for github org client.
# """
# import requests
# from functools import wraps
# from typing import (
#     Mapping,
#     Sequence,
#     Any,
#     Dict,
#     Callable,
# )

# __all__ = [
#     "access_nested_map",
#     "get_json",
#     "memoize",
# ]


# def access_nested_map(nested_map: Mapping, path: Sequence) -> Any:
#     """Access nested map with key path.
#     Parameters
#     ----------
#     nested_map: Mapping
#         A nested map
#     path: Sequence
#         a sequence of key representing a path to the value
#     Example
#     -------
#     >>> nested_map = {"a": {"b": {"c": 1}}}
#     >>> access_nested_map(nested_map, ["a", "b", "c"])
#     1
#     """
#     for key in path:
#         if not isinstance(nested_map, Mapping):
#             raise KeyError(key)
#         nested_map = nested_map[key]

#     return nested_map


# def get_json(url: str) -> Dict:
#     """Get JSON from remote URL.
#     """
#     response = requests.get(url)
#     return response.json()


# def memoize(fn: Callable) -> Callable:
#     """Decorator to memoize a method.
#     Example
#     -------
#     class MyClass:
#         @memoize
#         def a_method(self):
#             print("a_method called")
#             return 42
#     >>> my_object = MyClass()
#     >>> my_object.a_method
#     a_method called
#     42
#     >>> my_object.a_method
#     42
#     """
#     attr_name = "_{}".format(fn.__name__)

#     @wraps(fn)
#     def memoized(self):
#         """"memoized wraps"""
#         if not hasattr(self, attr_name):
#             setattr(self, attr_name, fn(self))
#         return getattr(self, attr_name)

#     return property(memoized)


# nested_map = {"a":{"b":{"c":3}}}
# print(access_nested_map(nested_map, ["a", "b", "c"]))


from parameterized import parameterized
import unittest
from typing import (
    Mapping,
    Sequence,
    Any,
)

def access_nested_map(nested_map: Mapping, path: Sequence) -> Any:

    for key in path:
        if not isinstance(nested_map, Mapping):
            raise KeyError(key)
        nested_map = nested_map[key]

    return nested_map


class TestAccessNestedMap(unittest.TestCase):
    """Test cases for access_nested_map function."""
    
    @parameterized.expand([
        ({"a": 1}, ("a",), 1),
        ({"a": {"b": 2}}, ("a",), {"b": 2}),
        ({"a": {"b": 2}}, ("a", "b"), 2),
        ({"a": {"b": {"c": 3}}}, ("a", "b", "c"), 3),
        ({}, ("a",), KeyError),
        ({"a":1}, ("a", "b"), KeyError)
    ])

    def test_access_nested_map(self, nested_map: Mapping, path: Sequence, expected: Any):
        """Test access_nested_map with various inputs."""

        if isinstance(expected, type) and issubclass(expected, Exception):
            with self.assertRaises(expected):
                access_nested_map(nested_map, path)
            
        else:        
            result = access_nested_map(nested_map, path)
            self.assertEqual(result, expected)
               

if __name__ == "__main__":
    unittest.main() 

