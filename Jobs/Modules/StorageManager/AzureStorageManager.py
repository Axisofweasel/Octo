import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


class StorageManager:
    
    def __init__(self) -> None:
        pass
    
    def blob_client(self, account_url):
        self.blob_service_client = BlobServiceClient(account_url)
        return

    def container_exists(self, storage_account, metadata=False):
        
        blob_service_client = self.blob_service_client
        containers = blob_service_client.list_containers(include_metadata=metadata)
        
        for container in containers:
            if container['name'] == storage_account:
                self.container_name = container['name']
                return True
        return False
    
    def upload_blob(self, blob_path, blob_file, file_path, file_name):
        
        try:
            blob = blob_path + blob_file
            blob_client = self.blob_service_client.get_blob_client(self.container_name,blob)
            full_path = file_path+file_name
        
            if os.path.exists(full_path):
                with open(file = full_path, mode ='rb') as data:
                    blob_client.upload_blob(data)
                    return True
            else:
                return False
        except FileNotFoundError:
            print(f"Error:{full_path} not found")
            return False
        except Exception as e:
            print(f"Error: {str(e)}")
            return False
