from typing import Any

from dagster import (
    AssetExecutionContext,
    FreshnessPolicy,
    MetadataValue,
    asset,
)

from .resources import GitHubAPIResource


@asset(
    name='delta-rs_repo_metadata',
    key_prefix=['stage', 'github', 'repositories', 'delta-io', 'delta-rs'],
    io_manager_key='json_io_manager',
    group_name='github',
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24),  # 24 hours
)
def delta_rs_metadata(context: AssetExecutionContext, github_api: GitHubAPIResource) -> dict[str, Any]:
    """Metadata from the GitHub repository of the Delta Lake Python client."""
    repo_metadata = github_api.get_repository(owner='delta-io', repo='delta-rs')

    context.add_output_metadata(
        metadata={
            'repo link': MetadataValue.url(repo_metadata.get('html_url')),
            'data preview': MetadataValue.json(repo_metadata),
        }
    )

    return repo_metadata


@asset(
    name='iceberg-python_repo_metadata',
    key_prefix=['stage', 'github', 'repositories', 'apache', 'iceberg-python'],
    io_manager_key='json_io_manager',
    group_name='github',
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24),  # 24 hours
)
def iceberg_python_metadata(context: AssetExecutionContext, github_api: GitHubAPIResource) -> dict[str, Any]:
    """Metadata from the GitHub repository of the Iceberg Python client."""
    repo_metadata = github_api.get_repository(owner='apache', repo='iceberg-python')

    context.add_output_metadata(
        metadata={
            'repo link': MetadataValue.url(repo_metadata.get('html_url')),
            'data preview': MetadataValue.json(repo_metadata),
        }
    )

    return repo_metadata
