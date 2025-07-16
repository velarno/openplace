import requests
from enum import Enum
from typing import Callable
from dataclasses import dataclass


FileWriter = Callable[[str, str, str, requests.Response], int]
"""
A function that writes a file to a storage and returns the file size.

Args:
    annonce_id (str): The ID of the announcement.
    filename (str): The name of the file.
    file_type (str): The type of the file.
    response (requests.Response): The response from the server.
    streaming (bool): Whether the response is streaming.

Returns:
    int: The file size.
"""

class WriterType(Enum):
    FS = "fs"  # Local filesystem
    S3 = "s3"  # AWS S3
    TEMP = "temp"  # Temporary directory


class StorageType(Enum):
    LOCAL = "local"  # Local filesystem
    S3 = "s3"  # AWS S3
    TEMP = "temp"  # Temporary directory


@dataclass
class ArchiveContent:
    """Raw markdown content of an archive file after extraction.
    """
    posting_id: int
    filename: str
    file_type: str
    content: str