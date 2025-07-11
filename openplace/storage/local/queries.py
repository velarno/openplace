from sqlmodel import Session, select
from typing import Sequence

from openplace.storage.local.models import PostingLink, ArchiveEntry, FetchingStatus, Posting
from openplace.storage.local.settings import connect_to_db

from functools import wraps
from typing import Callable, Any

from pathlib import Path
import zipfile
def ensure_session(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to ensure that a SQLModel Session is provided to the decorated function.
    If the session argument is None, it will create a new session using connect_to_db.

    Args:
        func (Callable[..., Any]): The function to decorate.

    Returns:
        Callable[..., Any]: The wrapped function with ensured session.
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        session: Session | None = kwargs.get("session", None)
        if session is None:
            _, session = connect_to_db()
            kwargs["session"] = session
        return func(*args, **kwargs)
    return wrapper

@ensure_session
def get_posting_links(posting_id: int, session: Session) -> Sequence[PostingLink]:
    """
    Get the links of a posting.
    """
    return session.exec(select(PostingLink).where(PostingLink.posting_id == posting_id)).all()

@ensure_session
def get_posting_links_by_kind(posting_id: int, kind: str, session: Session) -> Sequence[PostingLink]:
    """
    Get the links of a posting by kind.
    """
    return session.exec(select(PostingLink).where(PostingLink.posting_id == posting_id, PostingLink.kind == kind)).all()


@ensure_session
def create_zip_entries(
    zip_path: str, session: Session, posting_id: int
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
    session.add_all(entries)
    session.commit()
    session.refresh(entries)
    return entries

@ensure_session
def update_posting_fetching_status(posting_id: int, status: FetchingStatus, session: Session) -> None:
    """
    Update the fetching status of a posting.
    """
    posting = session.exec(select(Posting).where(Posting.id == posting_id)).first()
    if posting is None:
        raise ValueError(f"Posting with id {posting_id} not found")
    posting.fetching_status = status
    session.add(posting)
    session.commit()