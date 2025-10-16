import os
from typing import Union, List

from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()

def upload_to_azure(local_path, container_name=None):
    
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    container = container_name or os.getenv("AZURE_CONTAINER_NAME", "raw")

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )

    file_system_client = service_client.get_file_system_client(file_system=container)
    file_name = os.path.basename(local_path)

    try:
        file_system_client.delete_file(file_name)
    except Exception:
        pass

    file_client = file_system_client.create_file(file_name)

    with open(local_path, "rb") as f:
        data = f.read()
        file_client.append_data(data, offset=0, length=len(data))
        file_client.flush_data(len(data))

    print(f"Uploaded {file_name} to Azure Data Lake Gen2 container: {container}")

def _list_parquet_parts(path: str) -> List[str]:
    files = []
    for root, _dirs, filenames in os.walk(path):
        for filename in filenames:
            if filename.endswith(".parquet"):
                files.append(os.path.join(root, filename))
    return files


def upload_processed_to_azure(local_path: str):
    try:
        load_dotenv()

        account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        file_system = os.getenv("AZURE_CONTAINER_NAME")

        
        if not all([account_name, account_key, file_system]):
            raise ValueError("Missing Azure credentials or file system name in .env file")
        
        service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=account_key
        )

        file_system_client = service_client.get_file_system_client(file_system)

        if os.path.isdir(local_path):
            parquet_files = _list_parquet_parts(local_path)
            if not parquet_files:
                raise FileNotFoundError(f"No parquet files found under {local_path}")
            for parquet_file in parquet_files:
                rel_path = os.path.relpath(parquet_file, start=os.path.dirname(local_path))
                dest_path = f"processed/{rel_path}"
                with open(parquet_file, "rb") as file_data:
                    file_client = file_system_client.create_file(dest_path)
                    size = os.path.getsize(parquet_file)
                    file_client.append_data(data=file_data, offset=0, length=size)
                    file_client.flush_data(size)
                print(f"Uploaded processed file to Azure: {dest_path}")
        else:
            file_name = os.path.basename(local_path)
            dest_path = f"processed/{file_name}"
            with open(local_path, "rb") as file_data:
                file_client = file_system_client.create_file(dest_path)
                size = os.path.getsize(local_path)
                file_client.append_data(data=file_data, offset=0, length=size)
                file_client.flush_data(size)
            print(f"Uploaded processed file to Azure: {dest_path}")

    except Exception as e:
        print(f"Failed to upload processed file: {e}")
