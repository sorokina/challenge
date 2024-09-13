from typing import Any

from dagster import op


@op
def extract_metadata(repo_matadata: dict[str, Any]) -> dict[str, Any]:
    """Extracts list of fields from the repo metatdata and adds them to the report data dict.

    Args:
        - document_url (dict[str, Any]): \
            Metadata object for a GitHub repository.

    Returns:
        - dict[str, Any]: \
            Extracted data for the report.
    """
    extracted_data = {
        'stars': repo_matadata.get('stargazers_count'),
        'forks': repo_matadata.get('forks_count'),
        'watchers': repo_matadata.get('watchers_count'),
        'open issues': repo_matadata.get('open_issues_count'),
        'closed issues': None,  # no idea yet, have to research
        'avg days until issue was closed': None,  # well, I give up
        'open PRs': None,
        'closed PRs': None,
        'avg days until PR was closed': None,
        'number of contributors': None,
    }
    return extracted_data
