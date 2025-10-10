import os
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()

def upload_to_azure(local_path, container_name=None):
    """
    Uploads a local file to Azure Data Lake Gen2.
    """
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    container = container_name or os.getenv("AZURE_CONTAINER_NAME", "raw")

    # ADLS Gen2 istemcisi
    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )

    file_system_client = service_client.get_file_system_client(file_system=container)
    file_name = os.path.basename(local_path)

    # Eğer aynı isimde dosya varsa overwrite et
    try:
        file_system_client.delete_file(file_name)
    except Exception:
        pass

    # Dosyayı oluştur ve yükle
    file_client = file_system_client.create_file(file_name)

    with open(local_path, "rb") as f:
        data = f.read()
        file_client.append_data(data, offset=0, length=len(data))
        file_client.flush_data(len(data))

    print(f"✅ Uploaded {file_name} to Azure Data Lake Gen2 container: {container}")
