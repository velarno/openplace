from sqlmodel import Session, select
from typing import Sequence, Optional

from openplace.storage.local.models import PostingLink, ArchiveEntry, FetchingStatus, Posting
from openplace.storage.local.settings import connect_to_db
from openplace.tasks.store.types import StorageType

from functools import wraps
from typing import Callable, Any

from pathlib import Path
import zipfile
import inspect

def ensure_session(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to ensure that a SQLModel Session is provided to the decorated function.
    If the session argument is None, it will create a new session using connect_to_db.
    """
    sig = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()
        session = bound.arguments.get("session", None)
        if session is None:
            _, session = connect_to_db()
            bound.arguments["session"] = session
        return func(*bound.args, **bound.kwargs)
    return wrapper

@ensure_session
def get_posting_links(posting_id: int, session: Optional[Session] = None) -> Sequence[PostingLink]:
    """
    Get the links of a posting.
    """
    if session is None:
        raise ValueError("Session is required")
    return session.exec(select(PostingLink).where(PostingLink.posting_id == posting_id)).all()

@ensure_session
def get_posting_links_by_kind(posting_id: int, kind: str, session: Optional[Session] = None) -> Sequence[PostingLink]:
    """
    Get the links of a posting by kind.
    """
    if session is None:
        raise ValueError("Session is required")
    return session.exec(select(PostingLink).where(PostingLink.posting_id == posting_id, PostingLink.kind == kind)).all()

@ensure_session
def list_postings(session: Optional[Session] = None, storage: StorageType = StorageType.LOCAL, limit: int = 100) -> Sequence[Posting]:
    """
    List all postings.

    Args:
        session (Session): SQLModel session for database operations.
        storage (StorageType): Storage type.
        limit (int): Maximum number of postings to return.

    Returns:
        Sequence[Posting]: List of postings.
    """
    if session is None:
        raise ValueError("Session is required")
    if storage == StorageType.LOCAL:
        return session.exec(select(Posting).limit(limit)).all()
    else:
        raise ValueError(f"Storage type {storage} not supported")

@ensure_session
def is_posting_present(posting_id: int, session: Optional[Session] = None) -> bool:
    """
    Check if a posting is present in the database.
    """
    if session is None:
        raise ValueError("Session is required")
    return session.exec(select(Posting).where(Posting.id == posting_id)).first() is not None

@ensure_session
def create_zip_entries(
    zip_path: str, posting_id: int, session: Optional[Session] = None
) -> Sequence[ArchiveEntry]:
    """
    Populate the ArchiveEntry table with the file tree of a zip archive.

    Args:
        zip_path (str): Path to the zip file.
        session (Session): SQLModel session for database operations.

    Returns:
        List[ArchiveEntry]: List of created ArchiveEntry objects.
    """
    entries: list[ArchiveEntry] = []

    with zipfile.ZipFile(zip_path) as zip_file:
        for file_path in zip_file.namelist():
            is_dir = file_path.endswith('/')
            normalized_path = file_path.rstrip('/')
            name = Path(normalized_path).name
            parent_path = Path(normalized_path).parent
            parent_entry = next((e for e in entries if e.path == str(parent_path)), None)
            entry = ArchiveEntry(
                name=name,
                path=normalized_path,
                parent_id=parent_entry.id if parent_entry else None,
                posting_id=posting_id,
                is_dir=is_dir,
                is_extracted=False,
            )
            entries.append(entry)
    if session is None:
        raise ValueError("Session is required")
    session.add_all(entries)
    session.commit()
    session.refresh(entries)
    return entries

@ensure_session
def update_posting_fetching_status(posting_id: int, status: FetchingStatus, session: Optional[Session] = None) -> None:
    """
    Update the fetching status of a posting.
    """
    if session is None:
        raise ValueError("Session is required")
    posting = session.exec(select(Posting).where(Posting.id == posting_id)).first()
    if posting is None:
        raise ValueError(f"Posting with id {posting_id} not found")
    posting.fetching_status = status
    session.add(posting)
    session.commit()

@ensure_session
def remove_posting(posting_id: int | str, session: Optional[Session] = None) -> None:
    """
    Remove a posting.

    Args:
        posting_id (int | str): ID of the posting to remove.
        session (Session): SQLModel session for database operations.
    """
    if session is None:
        raise ValueError("Session is required")
    posting = session.exec(select(Posting).where(Posting.id == posting_id)).first()
    if posting is None:
        raise ValueError(f"Posting with id {posting_id} not found")
    session.delete(posting)
    session.commit()