import re
from typing import Any, List

from jinja2 import BaseLoader, Environment
from pydantic import Field

from backend.data.block import Block, BlockCategory, BlockOutput, BlockSchema, BlockType
from backend.data.model import SchemaField
from backend.util.mock import MockObject

# Helper classes
class StorageProvider(str, Enum):
    #AWS = "aws"   -- Not implemented yet
    MINIO = "minio"



# S3 compliant storage blocks
class StoreObjectInS3(Block):
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
            category=BlockCategory.MISC,
            input_schema=StoreObjectInS3.Input(),
            output_schema=StoreObjectInS3.Output()
        )
    
    def _store_in_s3_AWS(self) -> BlockOutput:
        # Implementation goes here
        pass

    def _store_in_s3_MINIO(self, key: str, obj: Any, bucket: str, endpoint: str, access_key: str, secret_key: str) -> BlockOutput:
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
            success, error = self._store_in_s3_MINIO(input_data.key, input_data.obj, input_data.bucket, input_data.endpoint, input_data.access_key, input_data.secret_key)
            yield "success": success, "error": error
        else:
            yield "success": False, "error": "Provider not implemented yet." 
