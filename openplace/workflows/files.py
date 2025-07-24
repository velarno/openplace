import json
import logging
from pathlib import Path

import openplace.storage.local.queries as q

from rich.progress import track

from openplace.storage.local.settings import connect_to_db, create_tables
from openplace.storage.local.models import Posting, PostingLink
from openplace.tasks.store.types import StorageType, FileWriter
from openplace.tasks.store.writers import fs_writer, local_archive_name
from openplace.tasks.scrape.fetch import PostingFileFetcher

logger = logging.getLogger(__name__)

def fetch_posting_files(
    posting: Posting, 
    storage: StorageType = StorageType.LOCAL,
    file_writer: FileWriter = fs_writer
    ) -> list[PostingLink]:
    """
    Fetch the files of a posting.
    """
    logger.info(f"Starting fetch_posting_files for posting_id={posting.id}")
    if storage == StorageType.LOCAL:
        engine, session = connect_to_db()
        create_tables(engine)
        logger.debug("Connected to DB and ensured tables exist.")
    else:
        logger.error(f"Unsupported storage type: {storage}")
        raise ValueError(f"Unsupported storage type: {storage}")

    links = q.get_posting_links(posting.id, session)
    logger.debug(f"Fetched posting links for posting_id={posting.id}: {links}")

    fetcher = PostingFileFetcher(str(posting.id), posting.org_acronym, file_writer)
    for link in links:
        try:
            filename, file_size = fetcher(link.kind, link.url)
            logger.info(f"Fetched file for link={link.url}, filename={filename}, size={file_size}")
            if filename is not None:
                archive_name = local_archive_name(str(posting.id), filename, link.kind)
                q.record_archive_entries(archive_name, posting.id, session)
                logger.debug(f"Created zip entry for filename={filename}, posting_id={posting.id}")
        except Exception as e:
            logger.error(f"Error fetching file for link={link.url}: {e}")
            q.update_posting_fetching_status(posting.id, q.FetchingStatus.FAILURE, session)
            raise e
    q.update_posting_fetching_status(posting.id, q.FetchingStatus.SUCCESS, session)
    logger.info(f"Completed fetch_posting_files for posting_id={posting.id}")
    return links

def download_pending_files(
    storage: StorageType = StorageType.LOCAL,
    display_progress: bool = True,
    ) -> tuple[int, int]:
    """
    Download pending files. This fetches for each posting the available archives.
    Allows for custom file writers, e.g. to download to a remote storage.
    Only local FS writer is supported for now.

    Args:
        storage: Storage type.
        display_progress: Whether to display progress.

    Returns:
        Tuple of number of success and number of failures.
    """
    if storage == StorageType.LOCAL:
        engine, session = connect_to_db()
        create_tables(engine)
    else:
        raise ValueError(f"Unsupported storage type: {storage}")

    links_postings = q.get_pending_links(session=session)

    records = (
        links_postings if not display_progress
        else track(links_postings, description="Retrieving pending tasks", total=len(links_postings))
    )

    num_success, num_failure = 0, 0

    for posting, link in records:
        fetcher = PostingFileFetcher(str(posting.id), posting.org_acronym, fs_writer)
        file_name, file_size = fetcher(link.kind, link.url)
        if file_name is not None:
            archive_name = local_archive_name(str(posting.id), file_name, link.kind)
            q.record_archive_entries(archive_name, posting.id, session)
            logger.debug(f"Created zip entry for filename={file_name}, posting_id={posting.id}")
            num_success += 1
            q.update_posting_fetching_status(posting.id, q.FetchingStatus.SUCCESS, session)
        else:
            num_failure += 1
            q.update_posting_fetching_status(posting.id, q.FetchingStatus.FAILURE, session)

        logger.info(f"Completed fetch_posting_files for posting_id={posting.id}")

    logger.info(f"Completed `retrieve_pending_tasks`, found {num_success} success and {num_failure} failures.")
    return num_success, num_failure


def ingest_labels(
    input_dir: str,
    id_source: str = "filename",
) -> None:
    """
    Ingest labels from a directory.
    """
    source_dir = Path(input_dir)
    if id_source == "filename":
        def get_id(file: Path) -> int:
            return int(next(part for part in file.name.split(".") if part.isdigit()))
    else:
        raise ValueError(f"Invalid id_source: {id_source}")
    if not source_dir.exists():
        raise FileNotFoundError(f"Input directory {input_dir} does not exist")
    if not source_dir.is_dir():
        raise NotADirectoryError(f"Input directory {input_dir} is not a directory")
    for file in source_dir.glob("*.jsonl"):
        file_id = get_id(file)
        data = json.loads(file.read_text())
        q.insert_archive_labels(
            id=file_id,
            label_data=data,
        )
        q.set_archive_content_inference_done(file_id)
        logger.info(f"Ingested labels for file {file} with id {file_id}")