from typing import Any

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue


def create_markdown_report(context: AssetExecutionContext, report_data: dict[str, dict]) -> str:
    """Create a markdown report from the report data.

    Args:
        - context (AssetExecutionContext): \
            Asset execution context.
        - report_data (dict[str, dict]): \
            Data for the report as a dictionary.

    Returns:
        - str: \
            Markdown formatted report.
    """
    # use pandas to convert dict with report data to markdown table
    df_report = pd.DataFrame.from_dict(report_data)
    md_report = df_report.to_markdown()

    context.add_output_metadata(
        metadata={
            'report': MetadataValue.md(md_report),
        }
    )

    return md_report


def extract_metadata(repo_metadata: dict[str, Any]) -> dict[str, Any]:
    """Extracts list of fields from the repo metatdata, issues and PRs and adds them to the report data dict.

    Args:
        - repo_matadata (dict[str, Any]): \
            Metadata object for a GitHub repository.

    Returns:
        - dict[str, Any]: \
            Extracted data from one repository as a column for the report.
    """
    # set all fields for the report

    extracted_data = {
        'stars': repo_metadata.get('stargazers_count'),
        'forks': repo_metadata.get('forks_count'),
        'watchers': repo_metadata.get('subscribers_count'),
        'releases': repo_metadata.get('releases'),
        'open issues': repo_metadata.get('open_issues_count'),
        'closed issues': repo_metadata.get('closed_issues'),
        'avg days until issue was closed': repo_metadata.get('avg_days_until_issue_was_closed'),
        'open PRs': repo_metadata.get('open_PRs'),
        'closed PRs': repo_metadata.get('closed_PRs'),
        'avg days until PR was closed': repo_metadata.get('avg_days_until_pr_was_closed'),
    }

    return extracted_data
