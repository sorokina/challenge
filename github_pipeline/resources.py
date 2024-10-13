from typing import Any, List, Dict
from urllib.parse import urljoin
from datetime import datetime
import time
import os
from dotenv import load_dotenv
import requests
from dagster import ConfigurableResource, get_dagster_logger

# Load environment variables from .env file
load_dotenv()

class GitHubAPIResource(ConfigurableResource):
    """Custom Dagster resource for the GitHub REST API."""

    # Load the token from environment variables
    github_token: str | None = os.getenv('GITHUB_TOKEN')
    """GitHub token for authentication. If no token is set, the API calls will be without authentication."""

    host: str = 'https://api.github.com'
    """Host of the GitHub REST API."""

    def execute_request(
        self,
        method: str,
        path: str,
        params: dict | list[tuple] | None = None,
        json: Any | None = None,
    ) -> requests.Response:
        """Execute a request to the GitHub REST API."""

        if params is None:
            params = {}
        default_params = {'apiVersion': '2022-11-28'}
        # passed parameters win over the default parameters
        params = {**default_params, **params}

        headers = {'Accept': 'application/vnd.github+json'}
        if self.github_token:
            headers['Authorization'] = f'Bearer {self.github_token}'
            get_dagster_logger().info("Using GitHub token for authentication.")
        else:
            get_dagster_logger().warning("No GitHub token found. Making unauthenticated requests, which may be limited.")

        try:
            response = requests.request(
                method=method,
                url=urljoin(self.host, path),
                params=params,
                headers=headers,
                json=json,
            )
            get_dagster_logger().info(f'Call {method}: {response.url}')

            response.raise_for_status()

        except requests.exceptions.HTTPError as err:
            get_dagster_logger().exception(f'{err!r} - {response.text}')
            raise  # Re-raise the exception after logging it

        return response


    def get_rate_limit(self) -> dict[str, Any]:
        """
        Get the current rate limit status.
        Docs: https://docs.github.com/en/rest/rate-limit#get-rate-limit-status
        Returns:
            dict[str, Any]: The rate limit status.
        """
        path = '/rate_limit'
        response = self.execute_request(method='GET', path=path)
        return response.json()

    def get_repository(self, owner: str, repo: str) -> dict[str, Any]:
        """Get metadata about a GitHub repository."""
        path = f'/repos/{owner}/{repo}'
        response = self.execute_request(method='GET', path=path)
        payload = response.json()
        return payload

    def get_releases(self, owner: str, repo: str) -> List[dict[str, Any]]:
        """Fetches all releases for a given repository, incorporating pagination and rate limit handling."""
        releases = []
        page = 1
        per_page = 100  # Maximum allowed by GitHub API

        while True:
            path = f'/repos/{owner}/{repo}/releases'
            params = {'page': page, 'per_page': per_page}
            response = self.execute_request(method='GET', path=path, params=params)

            # Check for rate limit errors
            if response.status_code == 403 and 'X-RateLimit-Reset' in response.headers:
                reset_time = int(response.headers['X-RateLimit-Reset'])
                current_time = int(time.time())
                sleep_duration = max(reset_time - current_time, 0) + 1
                get_dagster_logger().info(f"Rate limit exceeded. Sleeping for {sleep_duration} seconds.")
                time.sleep(sleep_duration)
                continue

            if response.status_code != 200:
                response.raise_for_status()

            payload = response.json()
            if not payload:
                break

            releases.extend(payload)
            page += 1
            time.sleep(1)  # Respect rate limits

        return releases

    def get_issues(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """Fetches all issues (excluding pull requests) for a specified repository, incorporating pagination and rate limit handling."""
        issues = []
        page = 1
        per_page = 100  # Maximum allowed by GitHub API

        while True:
            path = f'/repos/{owner}/{repo}/issues?state=all&page={page}&per_page={per_page}'
            try:
                response = self.execute_request(method='GET', path=path)
                response.raise_for_status()

                page_issues = response.json()
                if not page_issues:
                    break

                # Filter out pull requests (issues with a "pull_request" key)
                issues_only = [issue for issue in page_issues if 'pull_request' not in issue]
                issues.extend(issues_only)
                page += 1

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    get_dagster_logger().info("Rate limit exceeded. Checking rate limit status...")
                    rate_limit_info = self.get_rate_limit()
                    reset_time = rate_limit_info['rate']['reset']
                    current_time = int(time.time())
                    wait_time = reset_time - current_time + 1

                    get_dagster_logger().info(f"Waiting for {wait_time} seconds until rate limit resets...")
                    time.sleep(wait_time)
                else:
                    get_dagster_logger().exception(f"HTTP error occurred: {e}")
                    raise

            except Exception as e:
                get_dagster_logger().exception(f"An error occurred: {e}")
                raise

        return issues

    def get_pull_requests(self, owner: str, repo: str) -> List[dict[str, Any]]:
        """Fetches all pull requests for a specified repository, incorporating pagination and rate limit handling."""
        pull_requests = []
        page = 1
        per_page = 100  # Maximum allowed by GitHub API

        while True:
            path = f'/repos/{owner}/{repo}/pulls?state=all&page={page}&per_page={per_page}'
            response = self.execute_request(method='GET', path=path)

            if response.status_code == 403 and 'X-RateLimit-Reset' in response.headers:
                reset_time = int(response.headers['X-RateLimit-Reset'])
                current_time = int(time.time())
                sleep_duration = max(reset_time - current_time, 0) + 1
                get_dagster_logger().info(f"Rate limit exceeded. Sleeping for {sleep_duration} seconds.")
                time.sleep(sleep_duration)
                continue

            if response.status_code != 200:
                response.raise_for_status()

            page_pull_requests = response.json()
            if not page_pull_requests:
                break

            pull_requests.extend(page_pull_requests)
            page += 1
            time.sleep(1)

        return pull_requests






def calculate_avg_days_to_close_issues_or_prs(issues: List[Dict[str, Any]]) -> float:
    """for every closed issue or pull request calculates duration ("days_to_close"), sum it up over all issues and divides through total number of closed issues"""
    total_days = 0
    closed_issue_count = 0

    for issue in issues:
        created_at_str = issue.get('created_at')
        closed_at_str = issue.get('closed_at')

        if created_at_str and closed_at_str:
            created_at = datetime.strptime(created_at_str, "%Y-%m-%dT%H:%M:%SZ")
            closed_at = datetime.strptime(closed_at_str, "%Y-%m-%dT%H:%M:%SZ")
            days_to_close = (closed_at - created_at).days
            total_days += days_to_close
            closed_issue_count += 1

    if closed_issue_count > 0:
        avg_days = total_days / closed_issue_count
        return round(avg_days, 1)
    else:
        return 0.0
