import datetime
import io
import json
from typing import TYPE_CHECKING, Any

from dagster import (
    InitResourceContext,
    InputContext,
    OutputContext,
    io_manager,
)
from dagster_aws.s3 import PickledObjectS3IOManager
from upath import UPath

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class JsonObjectS3IOManager(PickledObjectS3IOManager):
    """Custom Dagster IOManager for writing / reading JSON files to / from S3.

    Args:
        - s3_bucket (str): \
            Name of the bucket, where the JSON files are stored.
        - s3_session (S3Client): \
            S3 client object created from `boto3.client("s3")`.
        - s3_prefix (str): \
            Path prefix for operations in the bucket. Otherwise, should be set to empty string `''`, but not to `None`.
    """

    extension: str = '.json'

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        """Loads JSON serializable object from S3 path.

        Args:
            - context (InputContext): \
                The context object from the asset function, which consumes the loaded asset as input.
            - path (upath.UPath): \
                Path from which the objects should be loaded from the S3 bucket.

        Returns:
            - Any: Serialized JSON object.

        Raises:
            - botocore.exceptions.NoSuchKey: When the file was not found in the S3 bucket.
            - ValueError: When the file from S3 is not JSON serializable.
        """
        try:
            s3_client: 'S3Client' = self.s3
            s3_obj = s3_client.get_object(Bucket=self.bucket, Key=path.as_posix())
            json_object = json.load(s3_obj['Body'])
            return context.dagster_type.typing_type(**json_object)
        except s3_client.exceptions.NoSuchKey:
            raise FileNotFoundError(f'Could not find file {path} in S3 bucket {self.bucket}')
        except json.JSONDecodeError as e:
            raise ValueError(f'File {path} in S3 bucket {self.bucket} does not contain valid JSON. Message: {e.msg}')

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        """Saves object to S3 path.

        Args:
            - context (dagster.OutputContext): \
                The context object from the asset function, which produces the asset as output.
            - obj (Any): \
                Object, which should be saved to the S3 bucket.
            - path (upath.UPath): \
                Path under which the objects should be saved in the S3 bucket.

        Raises:
            - TypeError: When passed `obj` is not JSON serializable.
        """
        s3_client: 'S3Client' = self.s3
        json_bytes = json.dumps(obj).encode('utf-8')
        # save normal json file (which gets overwritten on every dagster run)
        with io.BytesIO(json_bytes) as fp:
            s3_client.upload_fileobj(
                Fileobj=fp,
                Bucket=self.bucket,
                Key=path.as_posix(),
                ExtraArgs={'ContentType': 'application/json'},
            )
        # save backup json file with timestamp additionally, if configured
        if context.op_def.tags.get('activate_versioning', False):
            with io.BytesIO(json_bytes) as fp:
                backup_path = path.__class__._from_parts(path.parts[:-1]).joinpath(
                    f'{datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")}_{path.parts[-1]}'
                )
                s3_client.upload_fileobj(
                    Fileobj=fp,
                    Bucket=self.bucket,
                    Key=backup_path.as_posix(),
                    ExtraArgs={'ContentType': 'application/json'},
                )


@io_manager(
    description='IO Manager for storing JSON files in S3.',
    required_resource_keys={'s3'},
)
def s3_json_io_manager(init_context: InitResourceContext) -> JsonObjectS3IOManager:
    """Persistent IO manager using S3 for storage of JSON files.

    Saves JSON serializable objects. Suitable for objects storage for distributed executors, so long
    as each execution node has network connectivity and credentials for S3 and the backing bucket.

    Subsequent materializations of an asset will overwrite previous materializations of that asset.
    With a base directory of "/my/base/path", an asset with key
    `AssetKey(["one", "two", "three"])` would be stored in a file called "three" in a directory
    with path "/my/base/path/one/two/".

    Example usage:

    1. Attach this IO manager to a set of assets.

    .. code-block:: python

        from dagster import Definitions, asset
        from dagster_aws.s3 import s3_resource


        @asset
        def asset1():
            # create df ...
            return df

        @asset
        def asset2(asset1):
            return asset1[:5]

        defs = Definitions(
            assets=[asset1, asset2],
            resources={
                "io_manager": s3_json_io_manager.configured(
                    {"s3_bucket": "my-cool-bucket", "s3_prefix": "my-cool-prefix"}
                ),
                "s3": s3_resource,
            },
        )


    2. Attach this IO manager to your job to make it available to your ops.

    .. code-block:: python

        from dagster import job
        from dagster_aws.s3 import s3_resource

        @job(
            resource_defs={
                "io_manager": s3_json_io_manager.configured(
                    {"s3_bucket": "my-cool-bucket", "s3_prefix": "my-cool-prefix"}
                ),
                "s3": s3_resource,
            },
        )
        def my_job():
            ...

    Args:
        - init_context (dagster.InitResourceContext): \
            Configuration provided from the definitions file.

    Returns:
        - JsonObjectS3IOManager: \
            S3 IO Manager for handling JSON files.
    """
    s3_session = init_context.resources.s3
    s3_bucket = init_context.resource_config['s3_bucket']
    s3_prefix = init_context.resource_config.get('s3_prefix', '')  # should be set to empty string '', not to None.
    io_manager = JsonObjectS3IOManager(s3_bucket=s3_bucket, s3_session=s3_session, s3_prefix=s3_prefix)
    return io_manager
