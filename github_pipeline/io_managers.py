import datetime
import io
import json
from typing import TYPE_CHECKING, Any

from dagster import (
    InitResourceContext,
    InputContext,
    MetadataValue,
    OutputContext,
    io_manager,
)
from dagster import (
    _check as check,
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
            Additional path prefix for objects in the bucket. Otherwise, should be set to empty string `''`, but not to `None`.
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
            return json_object
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


class TextObjectS3IOManager(PickledObjectS3IOManager):
    """Custom Dagster IOManager for writing / reading text files to / from S3.

    Args:
        - s3_bucket (str): \
            Name of the bucket, where the text files are stored.
        - s3_session (S3Client): \
            S3 client object created from `boto3.client("s3")`.
        - s3_prefix (str): \
            Additional path prefix for objects in the bucket. Otherwise, should be set to empty string `''`, but not to `None`.
    """

    extension: str = '.txt'

    def get_path(self, context: InputContext | OutputContext) -> UPath:
        # set custom extension
        self.extension = context.op_def.tags.get('file_extension', '.txt')

        # rebuild path
        if context.has_asset_partitions:
            paths = self._get_paths_for_partitions(context)

            check.invariant(
                len(paths) == 1,
                f'The current IO manager {type(self)} does not support persisting an output'
                ' associated with multiple partitions. This error is likely occurring because a'
                " backfill was launched using the 'single run' option. Instead, launch the"
                " backfill with the 'multiple runs' option.",
            )

            path = next(iter(paths.values()))
            self._handle_transition_to_partitioned_asset(context, path.parent)
        else:
            path = self._get_path(context)

        # update path in metadata, if it exists
        if context.metadata.get('path'):
            context.metadata['path'] = MetadataValue.path(str(path))

        return path

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        """Loads text content of object from S3 path.

        Args:
            - context (InputContext): \
                The context object from the asset function, which consumes the loaded asset as input.
            - path (upath.UPath): \
                Path from which the objects should be loaded from the S3 bucket.

        Returns:
            - Any: Text content from file.

        Raises:
            - botocore.exceptions.NoSuchKey: When the file was not found in the S3 bucket.
        """
        # get path with custom file extension
        path = self.get_path(context=context)
        try:
            s3_client: 'S3Client' = self.s3
            s3_obj = s3_client.get_object(Bucket=self.bucket, Key=path.as_posix())
            text = s3_obj['Body'].encode('utf-8')
            return text
        except s3_client.exceptions.NoSuchKey:
            raise FileNotFoundError(f'Could not find file {path} in S3 bucket {self.bucket}')

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        """Saves object to S3 path.

        Args:
            - context (dagster.OutputContext): \
                The context object from the asset function, which produces the asset as output.
            - obj (Any): \
                Object, which should be saved to the S3 bucket.
            - path (upath.UPath): \
                Path under which the objects should be saved in the S3 bucket.
        """
        # get path with custom file extension
        path = self.get_path(context=context)
        s3_client: 'S3Client' = self.s3
        text_bytes = obj.encode('utf-8')
        # save normal json file (which gets overwritten on every dagster run)
        with io.BytesIO(text_bytes) as fp:
            s3_client.upload_fileobj(
                Fileobj=fp,
                Bucket=self.bucket,
                Key=path.as_posix(),
                ExtraArgs={'ContentType': 'text/plain'},
            )
        # save backup json file with timestamp additionally, if configured
        if context.op_def.tags.get('activate_versioning', False):
            with io.BytesIO(text_bytes) as fp:
                backup_path = path.__class__._from_parts(path.parts[:-1]).joinpath(
                    f'{datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")}_{path.parts[-1]}'
                )
                s3_client.upload_fileobj(
                    Fileobj=fp,
                    Bucket=self.bucket,
                    Key=backup_path.as_posix(),
                    ExtraArgs={'ContentType': 'text/plain'},
                )


@io_manager(
    description='IO Manager for storing files in S3.',
    required_resource_keys={'s3'},
)
def s3_io_manager(init_context: InitResourceContext) -> JsonObjectS3IOManager:
    """Persistent IO manager using S3 for storage of files.

    Currently supported data types:
    - `json`: a JSON serializable object (e.g. dict, list) can be stored as `<asset_key>.json`
    - `text`: an arbitrary string can be stored as text file as `<asset_key>.txt`
    - `pickle`: everthing else would be stored as pickled object as `<asset_key>.pkl`

    Needs following config values:
    - data_type (str): data type from list above, defaults to 'pickle'
    - s3_bucket (str): name of the S3 bucket, the data should be stored in
    - (optional) s3_prefix (str): Additional path prefix for objects in the bucket
    - (optional) file_extension (str): custom file extension e.g. '.md', otherwise default will be used (see list above)

    Saves file-based objects. Suitable for object storage for distributed executors, so long
    as each execution node has network connectivity and credentials for S3 and the backing bucket.

    With a base directory of "/my/base/path", an asset with key
    `AssetKey(["one", "two", "three"])` would be stored in a file called "three" in a directory
    with path "/my/base/path/one/two/".
    Subsequent materializations of an asset will overwrite previous materializations of that asset.
    If you add `op_tags={'activate_versioning': True}` to an asset definition, the file will be saved
    with the current timestamp as prefix as well.

    Example usage:

    1. Attach this IO manager to a set of assets.
    ```python
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
                "io_manager": s3_io_manager.configured(
                    {"s3_bucket": "my-cool-bucket", "s3_prefix": "my-cool-prefix"}
                ),
                "s3": s3_resource,
            },
        )
    ```

    2. Attach this IO manager to your job to make it available to your ops.
    ```python
        from dagster import job
        from dagster_aws.s3 import s3_resource

        @job(
            resource_defs={
                "io_manager": s3_io_manager.configured(
                    {"s3_bucket": "my-cool-bucket", "s3_prefix": "my-cool-prefix"}
                ),
                "s3": s3_resource,
            },
        )
        def my_job():
            ...
    ```

    Args:
        - init_context (dagster.InitResourceContext): \
            Configuration provided from the definitions file.

    Returns:
        - JsonObjectS3IOManager | TextObjectS3IOManager | PickledObjectS3IOManager: \
            S3 IO Manager for handling files.
    """
    s3_session = init_context.resources.s3
    data_type = init_context.resource_config['data_type']
    s3_bucket = init_context.resource_config['s3_bucket']
    s3_prefix = init_context.resource_config.get('s3_prefix', '')  # should be set to empty string '', not to None.
    file_extension = init_context.resource_config.get('file_extension')  # optional

    if data_type == 'json':
        io_manager = JsonObjectS3IOManager(s3_bucket=s3_bucket, s3_session=s3_session, s3_prefix=s3_prefix)
    elif data_type == 'text':
        io_manager = TextObjectS3IOManager(s3_bucket=s3_bucket, s3_session=s3_session, s3_prefix=s3_prefix)
    else:
        io_manager = PickledObjectS3IOManager(s3_bucket=s3_bucket, s3_session=s3_session, s3_prefix=s3_prefix)

    # set custom file extension
    if file_extension:
        io_manager.extension = file_extension

    return io_manager
