from azure.storage.blob.aio import BlobServiceClient
from app.config import settings
from app.core.logging import logger
from typing import Optional


class AzureBlobService:
    def __init__(self):
        self.blob_service_client = BlobServiceClient.from_connection_string(
            settings.AZURE_STORAGE_CONNECTION_STRING
        )
        self.container_name = settings.AZURE_CONTAINER_NAME

    async def upload_file(
        self,
        file_content: bytes,
        file_name: str,
        folder: str = "uploads"
    ) -> str:
        """Upload file to Azure Blob Storage"""
        try:
            container_client = self.blob_service_client.get_container_client(
                self.container_name
            )

            blob_path = f"{folder}/{file_name}"

            await container_client.upload_blob(
                name=blob_path,
                data=file_content,
                overwrite=True
            )

            blob_url = f"https://{self.blob_service_client.account_name}.blob.core.windows.net/{self.container_name}/{blob_path}"
            logger.info(f"File uploaded to Azure: {blob_path}")

            return blob_url

        except Exception as e:
            logger.error(f"Error uploading file to Azure: {e}")
            raise

    async def download_file(self, blob_path: str) -> bytes:
        """Download file from Azure Blob Storage"""
        try:
            container_client = self.blob_service_client.get_container_client(
                self.container_name
            )

            blob_client = container_client.get_blob_client(blob_path)
            download_stream = await blob_client.download_blob()

            return await download_stream.readall()

        except Exception as e:
            logger.error(f"Error downloading file from Azure: {e}")
            raise

    async def delete_file(self, blob_path: str) -> bool:
        """Delete file from Azure Blob Storage"""
        try:
            container_client = self.blob_service_client.get_container_client(
                self.container_name
            )

            await container_client.delete_blob(blob_path)
            logger.info(f"File deleted from Azure: {blob_path}")

            return True

        except Exception as e:
            logger.error(f"Error deleting file from Azure: {e}")
            raise
