from typing import Any, Generator, Optional
from urllib.parse import urljoin

import requests
from dagster import ConfigurableResource, get_dagster_logger


class GitHubAPIResource(ConfigurableResource):
    """Custom Dagster resource for the GitHub REST API.

    Args:
        - host (str, optional): \
            Host address of the Contentful Management API. Defaults to 'https://api.github.com'.
    """

    host: str = 'https://api.github.com'
    """Host of the GitHub REST API."""

    def execute_request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json: Any | None = None,
    ) -> requests.Response:
        """Execute a request to the GitHub REST API.

        Args:
            - method (str): \
                HTTP method for the API call, e.g. 'GET'.
            - path (str): \
                Path of the endpoint.
            - params (dict, optional): \
                Dictionary, list of tuples or bytes to send in the query string for the request.
            - json (Any, optional): \
                A JSON serializable Python object to send in the body of the request.

        Returns:
            - requests.Response: \
                Response object of the API call.
        """
        if params is None:
            params = {}
        default_params = {'apiVersion': '2022-11-28'}
        # passed parameters win over the default parameters
        params = {**default_params, **params}

        try:
            response = requests.request(
                method=method,
                url=urljoin(self.host, path),
                params=params,
                headers={'Accept': 'application/vnd.github+json'},
                json=json,
            )
            get_dagster_logger().info(f'Call {method}: {response.url}')

            response.raise_for_status()

        except requests.exceptions.HTTPError as err:
            get_dagster_logger().exception(f'{err!r} - {response.text}')

        return response

    def get_repository(self, owner: str, repo: str) -> dict[str, Any]:
        """Get metadata about a GitHub repository.
        Docs: https://docs.github.com/en/rest/repos/repos?apiVersion=2022-11-28#get-a-repository

        Args:
            - owner (str): \
                The account owner of the repository. The name is not case sensitive.
            - repo (str): \
                The name of the repository without the `.git` extension. The name is not case sensitive.

        Returns:
            - dict[str, Any]: \
                The metadata for the repository.
        """
        path = f'/repos/{owner}/{repo}'
        response = self.execute_request(method='GET', path=path)
        payload = response.json()

        return payload
