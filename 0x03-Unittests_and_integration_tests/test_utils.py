#!/usr/bin/env python3
# """Generic utilities for github org client.
# """
import requests
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


def get_json(url: str) -> dict:
    """Get JSON from remote URL.
    """
    response = requests.get(url)
    return response.json()


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




# Task 0. Write the first unit tests to test access_nested_map.
# Task1. Use parameterized to test the function with multiple inputs


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
               

# if __name__ == "__main__":
#     unittest.main() 



# Test 2. Mock HTTP calls


from unittest.mock import patch, Mock

class TestGetJson(unittest.TestCase):
    """Test cases for get_json function."""
    
    @patch('requests.get')
    def test_get_json(self, mock_get):
        """Test get_json with a mock response."""
        
        @parameterized.expand([
            ("http://example.com", {"payload": True}),
            ("http://holberton.io", {"payload": False})
        ])

        
        def test_get_json(self, test_url, test_payload, mock_get):
            """Test get_json with a mock response."""
            mock_response = Mock()
            mock_response.json.return_value = test_payload
            mock_get.return_value = mock_response

            result = get_json(test_url)
            self.assertEqual(result, test_payload)
            mock_get.assert_called_once_with(test_url)


# if __name__ == "__main__":
#     unittest.main()


# task 3. Implement memoization and write unittests for it.
from functools import wraps
from typing import Callable
def memoize(fn: Callable) -> Callable:

    attr_name = "_{}".format(fn.__name__)
    @wraps(fn)
    def wrapper(self):
        """memoized wraps"""
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    return property(wrapper)


class TestMemoize(unittest.TestCase):
    def test_memoize(self):
        class TestClass:
            def a_method(self):
                return 42
            
            @memoize
            def a_property(self):
                return self.a_method()
            

        test_instance = TestClass()

        with patch.object(test_instance, 'a_method', return_value=42) as mock_method:
            result1 = test_instance.a_property
            result2 = test_instance.a_property

            self.assertEqual(result1, 42)
            self.assertEqual(result2, 42)
            mock_method.assert_called_once()




if __name__ == "__main__":
    unittest.main()