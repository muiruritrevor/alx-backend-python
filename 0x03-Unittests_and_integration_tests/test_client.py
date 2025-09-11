#!/usr/bin/env python3
"""
A github org client
"""
from typing import (
    List,
    Dict,
)


from test_utils import (
    get_json,
    access_nested_map,
    memoize,
)


class GithubOrgClient:
    """
    A Githib org client
    """
    ORG_URL = "https://api.github.com/orgs/{org}"

    def __init__(self, org_name: str) -> None:
        """Init method of GithubOrgClient"""
        self._org_name = org_name

    @memoize
    def org(self) -> Dict:
        """Memoize org"""
        return get_json(self.ORG_URL.format(org=self._org_name))

    @property
    def _public_repos_url(self) -> str:
        """Public repos URL"""
        return self.org["repos_url"]

    @memoize
    def repos_payload(self) -> Dict:
        """Memoize repos payload"""
        return get_json(self._public_repos_url)

    def public_repos(self, license: str = None) -> List[str]:
        """Public repos"""
        json_payload = self.repos_payload
        public_repos = [
            repo["name"] for repo in json_payload
            if license is None or self.has_license(repo, license)
        ]

        return public_repos

    @staticmethod
    def has_license(repo: Dict[str, Dict], license_key: str) -> bool:
        """Static: has_license"""
        assert license_key is not None, "license_key cannot be None"
        try:
            has_license = access_nested_map(repo, ("license", "key")) == license_key
        except KeyError:
            return False
        return has_license
    



        

import unittest
from unittest.mock import patch
from parameterized import parameterized
from test_client import GithubOrgClient

class TestGithubOrgClient(unittest.TestCase):
    """
    Test class for GithubOrgClient
    """
    @parameterized.expand([
        ("google",),
        ("abc",)
    ])

    @patch('test_client.get_json')
    def test_org(self, org_name, mock_get_json):
        """Test org method"""

        # Build a fake response (payload)
        test_payload = {
            "login": org_name, 
            "repos_url": f"https://api.github.com/orgs/{org_name}/repos"
            }
        mock_get_json.return_value = test_payload # Mocking get_json to return the fake response

        client = GithubOrgClient(org_name) # Create an instance of GithubOrgClient
        self.assertEqual(client.org, test_payload) # Assert that the org method returns the fake response

        # Ensure get_json was called once with the correct URL
        mock_get_json.assert_called_once_with(
            f"https://api.github.com/orgs/{org_name}"
        )

if __name__ == "__main__":
    unittest.main()       