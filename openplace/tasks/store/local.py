import os
import requests
from typing import Any

def fs_writer(annonce_id: str, filename: str, file_type: str, response: requests.Response) -> int:
    """
    Write a file to the local filesystem.
    """
    with open(f'{annonce_id}_{filename}_{file_type}.zip', 'wb') as f:
        f.write(response.content)
    return len(response.content)

def s3_writer(annonce_id: str, filename: str, file_type: str, response: requests.Response) -> Any:
    """
    Write a file to S3.
    """
    raise NotImplementedError("S3 writer not implemented")

def temp_writer(annonce_id: str, filename: str, file_type: str, response: requests.Response) -> Any:
    """
    Write a file to the temporary directory.
    """
    raise NotImplementedError("Temp writer not implemented")