import logging
from datetime import datetime
from sqlmodel import Session, select, not_
from typing import Sequence, Optional, Iterator

from openplace.storage.local.models import PostingLink, ArchiveEntry, FetchingStatus, Posting, ArchiveContent, ArchiveLabel
from openplace.storage.local.settings import connect_to_db
from openplace.tasks.store.types import StorageType

from functools import wraps
from typing import Callable, Any

from pathlib import Path
import zipfile
import inspect

logger = logging.getLogger(__name__)

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
def list_postings(
    session: Optional[Session] = None,
    storage: StorageType = StorageType.LOCAL,
    limit: int = 100,
    status: Optional[FetchingStatus] = None,
) -> Sequence[Posting]:
    """
    List all postings.

    Args:
        session (Session): SQLModel session for database operations.
        storage (StorageType): Storage type.
        limit (int): Maximum number of postings to return.
        status (FetchingStatus): Status of the postings.
    Returns:
        Sequence[Posting]: List of postings.
    """
    if session is None:
        raise ValueError("Session is required")
    if storage == StorageType.LOCAL:
        query = select(Posting)
        if status is not None:
            query = query.where(Posting.fetching_status == status)
        return session.exec(query.limit(limit)).all()
    else:
        raise ValueError(f"Storage type {storage} not supported")

@ensure_session
def is_posting_present(posting_id: int, session: Optional[Session] = None) -> bool:
    """
    Check if a posting is present in the database.
    """
    if session is None:
        raise ValueError("Session is required")
    logger.debug(f"Checking if posting {posting_id} is present in the database")
    result = session.exec(select(Posting).where(Posting.id == posting_id)).first()
    logger.debug(f"Result: {'Found' if result is not None else 'Not found'}")
    return result is not None

@ensure_session
def record_archive_entries(
    path: str, posting_id: int,
    session: Optional[Session] = None,
    persist: bool = True,
) -> Sequence[ArchiveEntry]:
    """
    Record archive entries in the database.
    An archive entry is either a single file or a filetree as a zip archive.

    Args:
        path (str): Path to the archive file.
        posting_id (int): ID of the posting.
        session (Session): SQLModel session for database operations.
        persist (bool): Whether to persist the entries to the database.

    Returns:
        Sequence[ArchiveEntry]: The created ArchiveEntry objects.
    """
    if session is None:
        raise ValueError("Session is required")

    if zipfile.is_zipfile(path):
        entries = create_zip_entries(path, posting_id, session)
    else:
        entries = [record_file_entry(path, posting_id, session)]
    if persist:
        session.add_all(entries)
        session.commit()
    return entries

@ensure_session
def record_file_entry(path: str, posting_id: int, session: Optional[Session] = None) -> ArchiveEntry:
    """
    Record a file entry in the database.
    """
    if session is None:
        raise ValueError("Session is required")

    name = Path(path).name

    return ArchiveEntry(
        name=name,
        path=path,
        parent_id=None,
        posting_id=posting_id,
        is_dir=False,
        is_extracted=False,
    )
    
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

@ensure_session
def get_pending_postings(session: Optional[Session] = None) -> Sequence[Posting]:
    """
    Get all pending postings.
    """
    if session is None:
        raise ValueError("Session is required")
    return session.exec(select(Posting).where(Posting.fetching_status == FetchingStatus.PENDING)).all()

@ensure_session
def get_pending_links(
    limit: Optional[int] = None,
    session: Optional[Session] = None,
) -> Sequence[Sequence[Posting | PostingLink]]:
    """
    Get all pending links.

    Args:
        limit (int): Maximum number of links to return.
        session (Session): SQLModel session for database operations.

    Returns:
        Sequence[Sequence[Posting | PostingLink]]: List of postings and their links.
    """
    if session is None:
        raise ValueError("Session is required")
    query = (
        select(Posting, PostingLink)
        .join(PostingLink)
        .where(PostingLink.fetching_status == FetchingStatus.PENDING)
    )
    if limit is not None:
        query = query.limit(limit)
    return session.exec(query).all()


@ensure_session
def record_archive_content(
    path: str,
    content: str,
    posting_id: int,
    entry_id: Optional[int] = None,
    session: Optional[Session] = None,
    persist: bool = True,
) -> ArchiveContent:
    """
    Record the content of a file.
    """
    if session is None:
        raise ValueError("Session is required")
    archive_content = ArchiveContent(path=path, content=content, posting_id=posting_id, entry_id=entry_id)
    if persist:
        session.add(archive_content)
        session.commit()
    return archive_content

@ensure_session
def get_archive_entry_from_filename(
    filename: str,
    session: Optional[Session] = None,
) -> ArchiveEntry | None:
    """
    Get the archive entry from the filename.
    """
    if session is None:
        raise ValueError("Session is required")
    return session.exec(select(ArchiveEntry).where(ArchiveEntry.name == filename)).first()

@ensure_session
def get_archive_content_from_path(
    path: str,
    session: Optional[Session] = None,
) -> ArchiveContent | None:
    """
    Get the file content from the path.
    """
    if session is None:
        raise ValueError("Session is required")
    return session.exec(select(ArchiveContent).where(ArchiveContent.path == path)).first()

@ensure_session
def get_archive_content_by_id(
    id: int,
    session: Optional[Session] = None,
) -> ArchiveContent | None:
    """
    Get the file content from the id.

    Args:
        id (int): ID of the archive content.
        session (Session): SQLModel session for database operations.

    Returns:
        ArchiveContent | None: The archive content.
    """
    if session is None:
        raise ValueError("Session is required")
    return session.exec(select(ArchiveContent).where(ArchiveContent.id == id)).first()

@ensure_session
def paginate_archive_contents(
    limit: Optional[int] = None,
    session: Optional[Session] = None,
    batch_size: int = 100,
) -> Iterator[Sequence[ArchiveContent]]:
    """
    Paginate through all archive contents.

    Args:
        limit (Optional[int]): Maximum number of contents to return.
        session (Session): SQLModel session for database operations.
        batch_size (int): Number of contents to return in each batch.

    Returns:
        Iterator[Sequence[ArchiveContent]]: Iterator over batches of archive contents.
    """
    if session is None:
        raise ValueError("Session is required")
    if limit is not None:
        return session.exec(select(ArchiveContent).limit(limit)).all()
    else:
        pagination = 0
        while True:
            contents = session.exec(select(ArchiveContent).offset(pagination).limit(batch_size)).all()
            if len(contents) == 0:
                break
            yield contents
            pagination += batch_size

@ensure_session
def get_unprocessed_archive_contents(
    limit: Optional[int] = None,
    session: Optional[Session] = None,
) -> Sequence[ArchiveContent]:
    """
    Get the unprocessed archive contents.

    Args:
        limit (Optional[int]): Maximum number of contents to return.
        session (Session): SQLModel session for database operations.

    Returns:
        Sequence[ArchiveContent]: List of unprocessed archive contents.
    """
    if session is None:
        raise ValueError("Session is required")
    query = select(ArchiveContent).where(not_(ArchiveContent.is_inference_done))
    if limit is not None:
        query = query.limit(limit)
    return session.exec(query).all()


@ensure_session
def set_archive_content_inference_done(
    id: int,
    session: Optional[Session] = None,
) -> None:
    """
    Set the inference done flag of an archive content.
    """
    if session is None:
        raise ValueError("Session is required")
    archive_content = session.exec(select(ArchiveContent).where(ArchiveContent.id == id)).first()
    if archive_content is None:
        raise ValueError(f"Archive content with id {id} not found")
    archive_content.is_inference_done = True
    session.add(archive_content)
    session.commit()

@ensure_session
def exists_labels_for_archive(
    archive_id: int,
    session: Optional[Session] = None,
) -> bool:
    """
    Check if labels exist for an archive.
    """
    if session is None:
        raise ValueError("Session is required")
    return session.exec(select(ArchiveLabel).where(ArchiveLabel.archive_id == archive_id)).first() is not None

@ensure_session
def insert_archive_labels(
    archive_id: int,
    label_data: list[dict],
    session: Optional[Session] = None,
) -> None:
    """
    Insert archive labels.
    """
    if session is None:
        raise ValueError("Session is required")

    for label_row in label_data:
        session.add(ArchiveLabel(
            archive_id=archive_id,
            label=label_row["label"],
            score=label_row["score"],
            text=label_row["text"],
            start_position=label_row["start"],
            end_position=label_row["stop"],
        ))
    session.commit()

@ensure_session
def get_labels_from_metadata(
    archive_id: int,
    label: str,
    start_position: int,
    end_position: int,
    session: Optional[Session] = None,
) -> Sequence[ArchiveLabel]:
    """
    Get the labels from the metadata: only fetches the entries that match the label and the position (start and end).

    Args:
        archive_id (int): The ID of the archive.
        label (str): The label to search for.
        start_position (int): The start position of the label.
        end_position (int): The end position of the label.
        session (Session): The SQLModel session.
    """
    if session is None:
        raise ValueError("Session is required")
    return session.exec(
        select(ArchiveLabel)
        .where(
            ArchiveLabel.archive_id == archive_id,
            ArchiveLabel.label == label,
            ArchiveLabel.start_position == start_position,
            ArchiveLabel.end_position == end_position,
        )
    ).all()

@ensure_session
def update_archive_labels(
    archive_id: int,
    label_data: list[dict],
    session: Optional[Session] = None,
) -> None:
    """
    Update archive labels. Generally gets called when the labels are already present in the database.

    Args:
        archive_id (int): The ID of the archive.
        label_data (list[dict]): The list of label data.
        session (Session): The SQLModel session.
    """
    if session is None:
        raise ValueError("Session is required")
    for label_row in label_data:
        existing_labels = get_labels_from_metadata(
            archive_id=archive_id,
            label=label_row["label"],
            start_position=label_row["start"],
            end_position=label_row["stop"],
            session=session,
        )
        if len(existing_labels) > 1:
            logger.warning(f"Multiple labels found for {label_row['label']} at {label_row['start']}-{label_row['stop']}. Label instances should be unique.")
        if existing_labels:
            logger.info(f"Found {len(existing_labels)} labels for {label_row['label']} at {label_row['start']}-{label_row['stop']}, updating")
            for label in existing_labels:
                label.score = label_row["score"] # TODO: add a check to see if the score is the same as the existing score
                label.text = label_row["text"] # TODO: add a check to see if the text is the same as the existing text
                label.start_position = label_row["start"] # TODO: add a check to see if the start position is the same as the existing start position
                label.end_position = label_row["stop"] # TODO: add a check to see if the end position is the same as the existing end position
                label.updated_at = datetime.now()
                session.add(label)
        else:
            logger.info(f"No labels found for {label_row['label']} at {label_row['start']}-{label_row['stop']}, inserting")
            session.add(ArchiveLabel(
                archive_id=archive_id,
                label=label_row["label"],
                score=label_row["score"],
                text=label_row["text"],
                start_position=label_row["start"],
                end_position=label_row["stop"],
            ))
    session.commit()

@ensure_session
def upsert_archive_labels(
    archive_id: int,
    label_data: list[dict],
    session: Optional[Session] = None,
) -> None:
    """
    Upsert archive labels. If the archive content has any existing labels, calls the update function.
    Otherwise, calls the insert function. Further optimization might be performed at a later stage.

    Args:
        archive_id (int): The ID of the archive.
        label_data (list[dict]): The list of label data.
        session (Session): The SQLModel session.
    """
    if session is None:
        raise ValueError("Session is required")
    if exists_labels_for_archive(archive_id, session):
        update_archive_labels(archive_id, label_data, session)
    else:
        insert_archive_labels(archive_id, label_data, session)