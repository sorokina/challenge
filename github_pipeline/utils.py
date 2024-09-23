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


def extract_metadata(repo_matadata: dict[str, Any]) -> dict[str, Any]:
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
        'stars': repo_matadata.get('stargazers_count'),
        'forks': repo_matadata.get('forks_count'),
        'watchers': repo_matadata.get('subscribers_count'),
        'releases': None,
        'open issues': repo_matadata.get('open_issues_count'),
        'closed issues': None, # no idea yet, have to research
        'avg days until issue was closed': None, # well, I give up
        'open PRs': None,
        'closed PRs': None,
        'avg days until PR was closed': None,
    }

    return extracted_data
