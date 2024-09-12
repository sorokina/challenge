from typing import Any
from dagster import (
    AssetExecutionContext,
    Backoff,
    Jitter,
    MetadataValue,
    RetryPolicy,
    asset,
)

from .resources import GitHubAPIResource


# asset factory function
def create_repo_metadata_assets(owner: str, repo: str):
    @asset(
        name=repo,
        key_prefix=['stage', 'github', 'repositories', owner],
        io_manager_key='json_io_manager',
        group_name='github',
        retry_policy=RetryPolicy(max_retries=3, delay=2, backoff=Backoff.EXPONENTIAL, jitter=Jitter.FULL),
        op_tags={'activate_versioning': True},
    )
    def _repo_metadata(context: AssetExecutionContext, github_api: GitHubAPIResource) -> dict[str, Any]:
        """Gets metadata for the GitHub repository."""
        repo_metadata = github_api.get_repository(owner=owner, repo=repo)

        context.add_output_metadata(
            metadata={
                'repo link': MetadataValue.url(repo_metadata.get('html_url')),
                'data preview': MetadataValue.json(repo_metadata),
            }
        )

        return repo_metadata

    return _repo_metadata


repositories = [
    {
        'owner': 'delta-io',
        'name': 'delta-rs',
    },
    {
        'owner': 'apache',
        'name': 'iceberg-python',
    },
]

assets = [
    create_repo_metadata_assets(owner=repository['owner'], repo=repository['name']) for repository in repositories
]
