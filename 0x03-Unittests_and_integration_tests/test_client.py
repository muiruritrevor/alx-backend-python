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
    


# Test 4. GithuborgClient.org method
import unittest
from unittest.mock import patch
from parameterized import parameterized, parameterized_class
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
            }
        mock_get_json.return_value = test_payload # Mocking get_json to return the fake response

        client = GithubOrgClient(org_name) # Create an instance of GithubOrgClient
        self.assertEqual(client.org, test_payload) # Assert that the org method returns the fake response

        # Ensure get_json was called once with the correct URL
        mock_get_json.assert_called_once_with(
            f"https://api.github.com/orgs/{org_name}"
        )

    # Test 5. Test _public_repos_url property

    def test_public_repos_url(self):
        """Test _public_repos_url property"""
        
        test_payload = {
            "login": "google", 
            "repos_url": "https://api.github.com/orgs/google/repos"
            }
        with patch('test_client.get_json', return_value=test_payload):
            client = GithubOrgClient("google")
            self.assertEqual(
                client._public_repos_url,
                "https://api.github.com/orgs/google/repos"
            )

# Test 6. Test public_repos method
    @patch('test_client.get_json')
    def test_public_repos(self, mock_get_json):
        """Test public_repos method"""
        
        test_payload = [
            {"name": "repo1"},
            {"name": "repo2"},
            {"name": "repo3"},
        ]
        mock_get_json.return_value = test_payload

        with patch.object(GithubOrgClient, '_public_repos_url', new_callable=unittest.mock.PropertyMock) as mock_public_repos_url:
            mock_public_repos_url.return_value = "https://api.github.com/orgs/google/repos"
            
            client = GithubOrgClient("google")
            self.assertEqual(client.public_repos(), ["repo1", "repo2", "repo3"])

            mock_public_repos_url.assert_called_once()


    # Test 7. Test public_repos with license

    @parameterized.expand([
        ({"name": "repo1", "license": {"key": "my_license"}}, "my_license", True),
        ({"name": "repo2", "license": {"key": "other_license"}}, "my_license", False),
        ({"name": "repo3"}, "my_license", False),
    ])

    def test_has_license(self, repo, license_key, expected):
        """Test has_license static method"""
        self.assertEqual(
            GithubOrgClient.has_license(repo, license_key),
            expected
        )


# Task 8. Integration test for GithubOrgClient public_repos
import requests
from fixtures import (
    org_payload,
    repos_payload,
    expected_repos,
    apache2_repos,
)

@parameterized_class([{
    'org_payload': org_payload,
    'repos_payload': repos_payload,
    'expected_repos': expected_repos,
    'apache2_repos': apache2_repos,

}])


class TestIntegrationGithubOrgClient(unittest.TestCase):
    """
    Integration test class for GithubOrgClient.
    Mocks only external HTTP requests via requests.get.
    """
    
    @classmethod
    def setUpClass(cls):
        """Set up the class with mocked HTTP responses."""
        cls.get_patcher = patch('test_client.requests.get')
        cls.mock_get = cls.get_patcher.start()

        # Mock the JSON responses for org and repos
        def side_effect(url):
            if url == "https://api.github.com/orgs/google":
                mock_resp = unittest.mock.Mock()
                mock_resp.json.return_value = cls.org_payload
                return mock_resp
            elif url == "https://api.github.com/orgs/google/repos":
                mock_resp = unittest.mock.Mock()
                mock_resp.json.return_value = cls.repos_payload
                return mock_resp
            else:
                raise ValueError("Unmocked URL: " + url)

        cls.mock_get.side_effect = side_effect

    @classmethod
    def tearDownClass(cls):
        """Tear down the class by stopping the patcher."""
        cls.get_patcher.stop()
    
    def test_public_repos(self):
        """Test public_repos method without license filter."""
        client = GithubOrgClient("google")
        self.assertEqual(client.public_repos(), self.expected_repos)

    def test_public_repos_with_license(self):
        """Test public_repos method with license filter."""
        client = GithubOrgClient("google")
        self.assertEqual(client.public_repos(license="Apache-2.0"), self.apache2_repos)


    
if __name__ == "__main__":
    unittest.main()       