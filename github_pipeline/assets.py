from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    FreshnessPolicy,
    MetadataValue,
    asset,
)

from .resources import GitHubAPIResource, calculate_avg_days_to_close_issues_or_prs
from .utils import create_markdown_report, extract_metadata


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


    releases = github_api.get_releases(owner='delta-io', repo='delta-rs')
    release_count = len(releases)

    repo_issues = github_api.get_issues(owner='delta-io', repo='delta-rs')
    # Filter for closed issues only
    closed_issues = [issue for issue in repo_issues if issue.get('state') == 'closed']
    closed_issues_count = len(closed_issues)

    pull_requests = github_api.get_pull_requests(owner='delta-io', repo='delta-rs')

    open_prs_count = sum(1 for pr in pull_requests if pr.get('state') == 'open')

    # Filter for closed pull requests only
    closed_prs = [pr for pr in pull_requests if pr.get('state') == 'closed']
    closed_prs_count = len(closed_prs)

    avg_days_to_close_issues = calculate_avg_days_to_close_issues_or_prs(closed_issues)
    avg_days_to_close_prs = calculate_avg_days_to_close_issues_or_prs(closed_prs)



    context.add_output_metadata(
        metadata={
            'repo link': MetadataValue.url(repo_metadata.get('html_url')),
            'data preview': MetadataValue.json(repo_metadata),
            'number of releases': MetadataValue.int(release_count),
            'closed issues': MetadataValue.int(closed_issues_count),
            'open PRs': MetadataValue.int(open_prs_count),
            'closed PRs': MetadataValue.int(closed_prs_count),
            'avg days until issue was closed': MetadataValue.float(avg_days_to_close_issues),
            'avg days until pr was closed': MetadataValue.float(avg_days_to_close_prs),
        }
    )

    repo_metadata['releases'] = release_count
    repo_metadata['closed_issues'] = closed_issues_count
    repo_metadata['open_PRs'] = open_prs_count
    repo_metadata['closed_PRs'] = closed_prs_count
    repo_metadata['avg_days_until_issue_was_closed'] = avg_days_to_close_issues
    repo_metadata['avg_days_until_pr_was_closed'] = avg_days_to_close_prs

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
    repo_metadata = github_api.get_repository(owner='apache', repo='iceberg-python'
)

    releases = github_api.get_releases(owner='apache', repo='iceberg-python')
    release_count = len(releases)

    repo_issues = github_api.get_issues(owner='apache', repo='iceberg-python')

    # Filter for closed issues only
    closed_issues = [issue for issue in repo_issues if issue.get('state') == 'closed']
    closed_issues_count = len(closed_issues)


    pull_requests = github_api.get_pull_requests(owner='apache', repo='iceberg-python')

    open_prs_count = sum(1 for pr in pull_requests if pr.get('state') == 'open')
    # Filter for closed pull requests only
    closed_prs = [pr for pr in pull_requests if pr.get('state') == 'closed']
    closed_prs_count = len(closed_prs)

    avg_days_to_close_issues = calculate_avg_days_to_close_issues_or_prs(closed_issues)
    avg_days_to_close_prs = calculate_avg_days_to_close_issues_or_prs(closed_prs)

    context.add_output_metadata(
        metadata={
            'repo link': MetadataValue.url(repo_metadata.get('html_url')),
            'data preview': MetadataValue.json(repo_metadata),
            'number of releases': MetadataValue.int(release_count),
            'closed issues': MetadataValue.int(closed_issues_count),
            'open PRs': MetadataValue.int(open_prs_count),
            'closed PRs': MetadataValue.int(closed_prs_count),
            'avg days until issue was closed': MetadataValue.float(avg_days_to_close_issues),
            'avg days until pr was closed': MetadataValue.float(avg_days_to_close_prs),
        }
    )
    repo_metadata['releases'] = release_count
    repo_metadata['closed_issues'] = closed_issues_count
    repo_metadata['open_PRs'] = open_prs_count
    repo_metadata['closed_PRs'] = closed_prs_count
    repo_metadata['avg_days_until_issue_was_closed'] = avg_days_to_close_issues
    repo_metadata['avg_days_until_pr_was_closed'] = avg_days_to_close_prs

    return repo_metadata


@asset(
    name='hudi-rs_repo_metadata',
    key_prefix=['stage', 'github', 'repositories', 'apache', 'hudi-rs'],
    io_manager_key='json_io_manager',
    group_name='github',
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24),  # 24 hours
)
def hudi_rs_metadata(context: AssetExecutionContext, github_api: GitHubAPIResource) -> dict[str, Any]:
    """Metadata from the GitHub repository of the Hudi Python client."""
    repo_metadata = github_api.get_repository(owner='apache', repo='hudi-rs')

    releases = github_api.get_releases(owner='apache', repo='hudi-rs')
    release_count = len(releases)

    repo_issues = github_api.get_issues(owner='apache', repo='hudi-rs')

    # Filter for closed issues only
    closed_issues = [issue for issue in repo_issues if issue.get('state') == 'closed']
    closed_issues_count = len(closed_issues)

    pull_requests = github_api.get_pull_requests(owner='apache', repo='hudi-rs')

    open_prs_count = sum(1 for pr in pull_requests if pr.get('state') == 'open')

    # Filter for closed pull requests only
    closed_prs = [pr for pr in pull_requests if pr.get('state') == 'closed']
    closed_prs_count = len(closed_prs)


    avg_days_to_close_issues = calculate_avg_days_to_close_issues_or_prs(closed_issues)
    avg_days_to_close_prs = calculate_avg_days_to_close_issues_or_prs(closed_prs)

    context.add_output_metadata(
        metadata={
            'repo link': MetadataValue.url(repo_metadata.get('html_url')),
            'data preview': MetadataValue.json(repo_metadata),
            'number of releases': MetadataValue.int(release_count),
            'closed issues': MetadataValue.int(closed_issues_count),
            'open PRs': MetadataValue.int(open_prs_count),
            'closed PRs': MetadataValue.int(closed_prs_count),
            'avg days until issue was closed': MetadataValue.float(avg_days_to_close_issues),
            'avg days until pr was closed': MetadataValue.float(avg_days_to_close_prs),
        }
    )
    repo_metadata['releases'] = release_count
    repo_metadata['closed_issues'] = closed_issues_count
    repo_metadata['open_PRs'] = open_prs_count
    repo_metadata['closed_PRs'] = closed_prs_count
    repo_metadata['avg_days_until_issue_was_closed'] = avg_days_to_close_issues
    repo_metadata['avg_days_until_pr_was_closed'] = avg_days_to_close_prs
    return repo_metadata





@asset(
    key_prefix=['dm', 'reports'],
    ins={
        'delta_rs': AssetIn(AssetKey('delta-rs_repo_metadata')),
        'iceberg_python': AssetIn(AssetKey('iceberg-python_repo_metadata')),
        'hudi_rs': AssetIn(AssetKey('hudi-rs_repo_metadata')),
    },
    io_manager_key='md_io_manager',
    group_name='github',
)
def repo_report(
    context: AssetExecutionContext, delta_rs: dict[str, Any], iceberg_python: dict[str, Any], hudi_rs: dict[str, Any]
) -> str:
    """Report for comparing GitHub repositories."""

    report_data = {
        'delta-rs': extract_metadata(repo_metadata=delta_rs),
        'iceberg-python': extract_metadata(repo_metadata=iceberg_python),
        'hudi-rs': extract_metadata(repo_metadata=hudi_rs),
    }

    return create_markdown_report(context=context, report_data=report_data)
