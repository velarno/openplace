from sqlmodel import Field, SQLModel
from datetime import datetime
from typing import Optional
from enum import Enum

class FetchingStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILURE = "failure"

class PostingPage(SQLModel, table=True):
    """
    Database model representing a PLACE public market posting page (raw HTML).

    Attributes:
        id (Optional[int]): Primary key.
        reference (str): Reference of the posting.
        url (str): URL of the posting.
        created_at (datetime): Creation timestamp.
        content (str): Raw HTML content of the posting.
    """
    id: int | None = Field(None, primary_key=True, index=True)
    url: str = Field(nullable=False)
    created_at: datetime = Field(nullable=False, default=datetime.now())
    content: str = Field(nullable=False)

class Posting(SQLModel, table=True):
    """
    Database model representing a PLACE public market posting.

    Attributes:
        id (Optional[int]): Primary key.
        reference (str): Reference of the posting.
        url (str): URL of the posting.
        title (str): Title of the posting.
        description (str): Description of the posting.
        organization (str): Organization of the posting.
        org_acronym (str): Acronym of the organization.
        last_updated (datetime): Last updated timestamp.
    """
    id: int | None = Field(None, primary_key=True, index=True)
    reference: str = Field(nullable=False)
    url: str = Field(nullable=False)
    title: str = Field(nullable=False)
    description: str = Field(nullable=False)
    organization: str = Field(nullable=False)
    org_acronym: str = Field(nullable=False)
    last_updated: datetime = Field(nullable=False, default=datetime.now())
    is_fetching_done: bool = Field(default=False, nullable=False)
    fetching_status: FetchingStatus = Field(default=FetchingStatus.PENDING, nullable=False)

class PostingLink(SQLModel, table=True):
    """
    Database model representing a link to a PLACE public market posting.

    When a Posting is deleted, all associated PostingLink records are also deleted (ON DELETE CASCADE).
    """
    id: int | None = Field(None, primary_key=True, index=True)
    posting_id: int = Field(
        nullable=False,
        foreign_key="posting.id",
        ondelete="CASCADE"
    )
    url: str = Field(nullable=False)
    kind: str = Field(nullable=False)
    fetching_status: FetchingStatus = Field(default=FetchingStatus.PENDING, nullable=False)
    last_updated: datetime = Field(nullable=False, default=datetime.now())

class ArchiveEntry(SQLModel, table=True):
    """
    Database model representing an entry (file or directory) in a zip archive.

    Attributes:
        id (Optional[int]): Primary key.
        name (str): Name of the file or directory.
        path (str): Full path within the zip archive.
        parent_id (Optional[int]): ID of the parent entry (None for root).
        is_dir (bool): True if entry is a directory, False if file.
        is_extracted (bool): Extraction status.
    """
    id: int | None = Field(None, primary_key=True, index=True)
    name: str = Field(nullable=False)
    path: str = Field(nullable=False, index=True)
    parent_id: Optional[int] = Field(foreign_key="archiveentry.id")
    posting_id: int = Field(nullable=False, foreign_key="posting.id")
    is_dir: bool = Field(nullable=False)
    is_extracted: bool = Field(default=False, nullable=False)

class ArchiveContent(SQLModel, table=True):
    """
    Database model representing the content of an archive file.
    """
    id: int | None = Field(None, primary_key=True, index=True)
    path: str = Field(nullable=False, index=True)
    content: str = Field(nullable=False)
    created_at: datetime = Field(nullable=False, default=datetime.now())
    updated_at: datetime = Field(nullable=False, default=datetime.now())
    posting_id: int = Field(nullable=False, foreign_key="posting.id")
    entry_id: Optional[int] = Field(nullable=True, foreign_key="archiveentry.id")
    is_inference_done: bool = Field(default=False, nullable=False)

class ArchiveLabel(SQLModel, table=True):
    """
    Database model representing the labels of an archive file.

    When a Posting is deleted, all associated ArchiveLabel records are also deleted (ON DELETE CASCADE).
    When an ArchiveEntry is deleted, all associated ArchiveLabel records are also deleted (ON DELETE CASCADE).
    """
    id: int | None = Field(None, primary_key=True, index=True)
    archive_id: int = Field(nullable=False, foreign_key="archiveentry.id", ondelete="CASCADE")
    start_position: int = Field(nullable=False)
    end_position: int = Field(nullable=False)
    label: str = Field(nullable=False)
    score: float = Field(nullable=False)
    text: str = Field(nullable=False)
    created_at: datetime = Field(nullable=False, default=datetime.now())
    updated_at: datetime = Field(nullable=False, default=datetime.now())