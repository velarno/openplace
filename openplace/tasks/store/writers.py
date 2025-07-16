import logging
import os
import re
import requests
from typing import Any
from pathlib import Path

logger = logging.getLogger(__name__)

def detect_content_type(response: requests.Response) -> str:
    """Detect the content type of a response"""
    return response.headers.get('Content-Type', 'application/octet-stream')

def local_archive_name(posting_id: str, filename: str, file_type: str) -> str:
    """Provides a standardized naming convention for local archives"""
    stem = Path(filename).stem
    return f'{posting_id}.{stem}.{file_type}.zip'

def parse_archive_name(archive_name: str) -> tuple[int, str, str]:
    """Parse the archive name into posting_id, filename, and file_type.

    Args:
        archive_name (str): The name of the archive.

    Returns:
        tuple[int, str, str]: The posting_id, filename, and file_type.
    """
    match = re.match(r'^(\d+)\.(.*)\.(.*)\.zip$', archive_name)
    if match:
        return int(match.group(1)), match.group(2), match.group(3)
    else:
        raise ValueError(f"Invalid archive name: {archive_name}")

def fs_writer(posting_id: str, filename: str, file_type: str, response: requests.Response, streaming: bool = False) -> int:
    """
    Write a file to the local filesystem.
    """
    content_type = detect_content_type(response)
    archive_name = local_archive_name(posting_id, filename, file_type)
    logger.debug(f"Writing file to {archive_name} ({content_type})")
    if streaming:
        with open(archive_name, 'wb') as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
    else:
        with open(archive_name, 'wb') as f:
            f.write(response.content)
    logger.debug(f"Wrote file to {archive_name}")
    return os.path.getsize(archive_name)

def s3_writer(posting_id: str, filename: str, file_type: str, response: requests.Response) -> Any:
    """
    Write a file to S3.
    """
    raise NotImplementedError("S3 writer not implemented")

def temp_writer(posting_id: str, filename: str, file_type: str, response: requests.Response) -> Any:
    """
    Write a file to the temporary directory.
    """
    raise NotImplementedError("Temp writer not implemented")