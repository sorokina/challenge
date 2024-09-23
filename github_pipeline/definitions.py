from dagster import (
    AssetSelection,
    Definitions,
    EnvVar,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_aws.s3 import s3_resource

from . import assets
from .io_managers import s3_io_manager
from .resources import GitHubAPIResource

all_assets = load_assets_from_modules([assets])

# Job for retrieving GitHub statistics
github_job = define_asset_job(name='refresh_repository_report', selection=AssetSelection.all())

defs = Definitions(
    assets=all_assets,
    jobs=[github_job],
    resources={
        'github_api': GitHubAPIResource(github_token=EnvVar('GITHUB_TOKEN').get_value()),
        'json_io_manager': s3_io_manager.configured(
            {'data_type': 'json', 's3_bucket': EnvVar('S3_BUCKET_NAME').get_value()}
        ),
        'text_io_manager': s3_io_manager.configured(
            {'data_type': 'text', 's3_bucket': EnvVar('S3_BUCKET_NAME').get_value()}
        ),
        's3': s3_resource,
    },
)
