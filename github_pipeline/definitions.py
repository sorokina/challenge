from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets
from .resources import GitHubAPIResource


all_assets = load_assets_from_modules([assets])

# Job for retrieving GitHub statistics
github_job = define_asset_job(name='refresh_repository_report', selection=AssetSelection.all())

defs = Definitions(
    assets=all_assets,
    jobs=[github_job],
    resources={
        'github_api': GitHubAPIResource(),
    },
)
