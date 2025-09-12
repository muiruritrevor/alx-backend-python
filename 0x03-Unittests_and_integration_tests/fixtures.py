# fixtures.py

org_payload = {
    "login": "google",
    "id": 1342004,
    "repos_url": "https://api.github.com/orgs/google/repos",
}

repos_payload = [
    {"id": 1, "name": "repo1", "license": {"key": "MIT"}},
    {"id": 2, "name": "repo2", "license": {"key": "Apache-2.0"}},
    {"id": 3, "name": "repo3", "license": {"key": "GPL-3.0"}},
]

# What we expect GithubOrgClient.public_repos() to return (just repo names)
expected_repos = ["repo1", "repo2", "repo3"]

# What we expect GithubOrgClient.public_repos("Apache-2.0") to return
apache2_repos = ["repo2"]
