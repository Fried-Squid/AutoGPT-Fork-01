import os
from enum import Enum
from typing import Any

from backend.data.block import (
    Block,
    BlockCategory,
    BlockInput,
    BlockOutput,
    BlockSchema,
)
from backend.data.model import SchemaField


# Helper classes
class StorageProvider(str, Enum):
    # AWS = "aws"   -- Not implemented yet
    MINIO = "minio"


# S3 compliant storage blocks
class StoreObjectInS3Block(Block):
    """
    This block is used to store an object in some S3 compliant storage.
    """

    class Input(BlockSchema):
        key: str = SchemaField(
            title="Key",
            description="The key to store the object under.",
            required=True,
        )
        obj: Any = SchemaField(
            title="Object",
            description="The object to store.",
            required=True,
        )
        provider: StorageProvider = SchemaField(
            title="Provider",
            description="The storage provider to use.",
            required=True,
            default=StorageProvider.MINIO,
        )
        bucket: str = SchemaField(
            title="Bucket",
            description="The bucket to store the object in.",
            required=True,
        )
        endpoint: str = SchemaField(
            title="Endpoint",
            description="The endpoint of the storage provider.",
            required=True,
            default="https://localhost:9000",
        )
        access_key: str = SchemaField(
            title="Access Key",
            description="The access key to use.",
            required=False,
        )
        secret_key: str = SchemaField(
            title="Secret Key",
            description="The secret key to use.",
            required=False,
        )

    class Output(BlockSchema):
        success: bool
        error: str

    def __init__(self):
        super().__init__(
            name="Store Object in S3",
            description="Store an object in an S3 compliant storage.",
            category=BlockCategory.OBJECT_STORAGE,
            input_schema=StoreObjectInS3.Input(),
            output_schema=StoreObjectInS3.Output(),
        )

    @staticmethod
    def _store_in_s3_AWS() -> BlockOutput:
        # Implementation goes here
        pass

    @staticmethod
    def _store_in_s3_MINIO(
        key: str,
        obj: Any,
        bucket: str,
        endpoint: str,
        access_key: str,
        secret_key: str,
    ) -> BlockOutput:
        from Minio import Minio
        from Minio.error import S3Error

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key)
        pwd = os.getcwd()
        source_file_path = pwd + "/temp"
        try:
            client.fput_object(bucket, key, source_file_path)
            success = True
            error = ""
        except S3Error as e:
            success = False
            error = str(e)

        return (success, error)

    def run(self, input_data: BlockInput) -> BlockOutput:
        provider = input_data.provider
        if provider == StorageProvider.MINIO:
            success, error = self._store_in_s3_MINIO(
                input_data.key,
                input_data.obj,
                input_data.bucket,
                input_data.endpoint,
                input_data.access_key,
                input_data.secret_key,
            )
            yield "success", success
            yield "error", error
        else:
            yield "success", False
            yield "error", "Provider not implemented yet."


class RetrieveObjectFromS3Block(Block):
    """
    This block is used to retrieve an object from some S3 compliant storage.
    """

    class Input(BlockSchema):
        key: str = SchemaField(
            title="Key",
            description="The key to retrieve the object under.",
            required=True,
        )
        provider: StorageProvider = SchemaField(
            title="Provider",
            description="The storage provider to use.",
            required=True,
            default=StorageProvider.MINIO,
        )
        bucket: str = SchemaField(
            title="Bucket",
            description="The bucket to retrieve the object from.",
            required=True,
        )
        endpoint: str = SchemaField(
            title="Endpoint",
            description="The endpoint of the storage provider.",
            required=True,
            default="https://localhost:9000",
        )
        access_key: str = SchemaField(
            title="Access Key",
            description="The access key to use.",
            required=False,
        )
        secret_key: str = SchemaField(
            title="Secret Key",
            description="The secret key to use.",
            required=False,
        )

    class Output(BlockSchema):
        obj: Any
        success: bool
        error: str

    def __init__(self):
        super().__init__(
            name="Retrieve Object from S3",
            description="Retrieve an object from an S3 compliant storage.",
            category=BlockCategory.OBJECT_STORAGE,
            input_schema=RetrieveObjectFromS3.Input(),
            output_schema=RetrieveObjectFromS3.Output(),
        )

    @staticmethod
    def _retrieve_from_s3_AWS() -> BlockOutput:
        # Implementation goes here
        pass

    @staticmethod
    def _retrieve_from_s3_MINIO(
        key: str, bucket: str, endpoint: str, access_key: str, secret_key: str
    ) -> BlockOutput:
        from Minio import Minio
        from Minio.error import S3Error

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key)
        try:
            obj = client.get_object(bucket, key)
            success = True
            error = ""
        except S3Error as e:
            obj = None
            success = False
            error = str(e)

        return (obj, success, error)

    def run(self, input_data: BlockInput) -> BlockOutput:
        provider = input_data.provider
        if provider == StorageProvider.MINIO:
            obj, success, error = self._retrieve_from_s3_MINIO(
                input_data.key,
                input_data.bucket,
                input_data.endpoint,
                input_data.access_key,
                input_data.secret_key,
            )
            if obj:
                yield "obj", obj
            yield "success", success
            yield "error", error
        else:
            yield "obj", None
            yield "success", False
            yield "error", "Provider not implemented yet."


class DeleteObjectFromS3Block(Block):
    """
    This block is used to delete an object from some S3 compliant storage.
    """

    class Input(BlockSchema):
        key: str = SchemaField(
            title="Key",
            description="The key to delete the object under.",
            required=True,
        )
        provider: StorageProvider = SchemaField(
            title="Provider",
            description="The storage provider to use.",
            required=True,
            default=StorageProvider.MINIO,
        )
        bucket: str = SchemaField(
            title="Bucket",
            description="The bucket to delete the object from.",
            required=True,
        )
        endpoint: str = SchemaField(
            title="Endpoint",
            description="The endpoint of the storage provider.",
            required=True,
            default="https://localhost:9000",
        )
        access_key: str = SchemaField(
            title="Access Key",
            description="The access key to use.",
            required=False,
        )
        secret_key: str = SchemaField(
            title="Secret Key",
            description="The secret key to use.",
            required=False,
        )

    class Output(BlockSchema):
        success: bool
        error: str

    def __init__(self):
        super().__init__(
            name="Delete Object from S3",
            description="Delete an object from an S3 compliant storage.",
            category=BlockCategory.OBJECT_STORAGE,
            input_schema=DeleteObjectFromS3.Input(),
            output_schema=DeleteObjectFromS3.Output(),
        )

    @staticmethod
    def _delete_from_s3_AWS() -> BlockOutput:
        # Implementation goes here
        pass

    @staticmethod
    def _delete_from_s3_MINIO(
        key: str, bucket: str, endpoint: str, access_key: str, secret_key: str
    ) -> BlockOutput:
        from Minio import Minio
        from Minio.error import S3Error

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key)
        try:
            client.remove_object(bucket, key)
            success = True
            error = ""
        except S3Error as e:
            success = False
            error = str(e)

        return (success, error)

    def run(self, input_data: BlockInput) -> BlockOutput:
        provider = input_data.provider
        if provider == StorageProvider.MINIO:
            success, error = self._delete_from_s3_MINIO(input_data.key, input_data)
            yield "success", success
            yield "error", error
        else:
            yield "success", False
            yield "error", "Provider not implemented yet."


class CreateBucketBlock(Block):
    """
    This block is used to create a bucket in some S3 compliant storage.
    """

    class Input(BlockSchema):
        bucket: str = SchemaField(
            title="Bucket",
            description="The bucket to create.",
            required=True,
        )
        provider: StorageProvider = SchemaField(
            title="Provider",
            description="The storage provider to use.",
            required=True,
            default=StorageProvider.MINIO,
        )
        endpoint: str = SchemaField(
            title="Endpoint",
            description="The endpoint of the storage provider.",
            required=True,
            default="https://localhost:9000",
        )
        access_key: str = SchemaField(
            title="Access Key",
            description="The access key to use.",
            required=False,
        )
        secret_key: str = SchemaField(
            title="Secret Key",
            description="The secret key to use.",
            required=False,
        )

    class Output(BlockSchema):
        success: bool
        error: str

    def __init__(self):
        super().__init__(
            name="Create Bucket",
            description="Create a bucket in an S3 compliant storage.",
            category=BlockCategory.OBJECT_STORAGE,
            input_schema=CreateBucket.Input(),
            output_schema=CreateBucket.Output(),
        )

    @staticmethod
    def _create_bucket_AWS() -> BlockOutput:
        # Implementation goes here
        pass

    @staticmethod
    def _create_bucket_MINIO(
        bucket: str, endpoint: str, access_key: str, secret_key: str
    ) -> BlockOutput:
        from Minio import Minio
        from Minio.error import S3Error

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key)
        try:
            client.make_bucket(bucket)
            success = True
            error = ""
        except S3Error as e:
            success = False
            error = str(e)

        return (success, error)

    def run(self, input_data: BlockInput) -> BlockOutput:
        provider = input_data.provider
        if provider == StorageProvider.MINIO:
            success, error = self._create_bucket_MINIO(
                input_data.bucket,
                input_data.endpoint,
                input_data.access_key,
                input_data.secret_key,
            )
            yield "success", success
            yield "error", error
        else:
            yield "success", False
            yield "error", "Provider not implemented yet."


class DeleteBucketBlock(Block):
    """
    This block is used to delete a bucket in some S3 compliant storage.
    """

    class Input(BlockSchema):
        bucket: str = SchemaField(
            title="Bucket",
            description="The bucket to delete.",
            required=True,
        )
        provider: StorageProvider = SchemaField(
            title="Provider",
            description="The storage provider to use.",
            required=True,
            default=StorageProvider.MINIO,
        )
        endpoint: str = SchemaField(
            title="Endpoint",
            description="The endpoint of the storage provider.",
            required=True,
            default="https://localhost:9000",
        )
        access_key: str = SchemaField(
            title="Access Key",
            description="The access key to use.",
            required=False,
        )
        secret_key: str = SchemaField(
            title="Secret Key",
            description="The secret key to use.",
            required=False,
        )

    class Output(BlockSchema):
        success: bool
        error: str

    def __init__(self):
        super().__init__(
            name="Delete Bucket",
            description="Delete a bucket in an S3 compliant storage.",
            category=BlockCategory.OBJECT_STORAGE,
            input_schema=DeleteBucket.Input(),
            output_schema=DeleteBucket.Output(),
        )

    @staticmethod
    def _delete_bucket_AWS() -> BlockOutput:
        # Implementation goes here
        pass

    @staticmethod
    def _delete_bucket_MINIO(
        bucket: str, endpoint: str, access_key: str, secret_key: str
    ) -> BlockOutput:
        from Minio import Minio
        from Minio.error import S3Error

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key)
        try:
            client.remove_bucket(bucket)
            success = True
            error = ""
        except S3Error as e:
            success = False
            error = str(e)

        return (success, error)
