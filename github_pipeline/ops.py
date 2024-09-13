from typing import Any

from dagster import op


@op
def extract_metadata(repo_matadata: dict[str, Any]) -> dict[str, Any]:
    """
    Downloads the PDF document from the document URL and uploads it to OBS.

    Args:
        - document_url (str): \
            Link to the document.
        - entry_id (str): \
            ID of the Contentful entry.
        - client (BaseClient): \
            A boto3 S3 client.
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
