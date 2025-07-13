import datetime
import requests
import logging
import openplace.tasks.scrape.fetch as fetch
import openplace.tasks.scrape.parse as parse
import openplace.tasks.scrape.navigate as navigate

from openplace.storage.local.models import Posting, PostingLink, FetchingStatus
from openplace.storage.local.settings import connect_to_db, create_tables
from openplace.tasks.store.types import StorageType, FileWriter
from openplace.tasks.store.writers import fs_writer
from openplace.tasks.scrape.fetch import PostingFileFetcher
from openplace.storage.local.queries import get_posting_links, create_zip_entries, update_posting_fetching_status, is_posting_present

logger = logging.getLogger(__name__)

def fetch_persist_posting(
    response: requests.Response,
    posting_id: str,
    org_acronym: str,
    storage: StorageType = StorageType.LOCAL,
) -> Posting | None:
    """
    Fetch and persist a PLACE public market posting.

    Args:
        response (requests.Response): The HTTP response object containing the posting page.
        posting_id (str): The ID of the posting.
        org_acronym (str): The acronym of the organization.
        storage (StorageType): The storage type.

    Returns:
        Posting | None: The persisted posting or None if the posting already exists.
    """
    logger.info(f"Starting fetch_persist_posting for posting_id={posting_id}")
    if storage == StorageType.LOCAL:
        engine, session = connect_to_db()
        create_tables(engine)
        if is_posting_present(posting_id, session):
            logger.info(f"Posting with id {posting_id} already present, skipping")
            return None
        else:
            logger.info(f"Posting with id {posting_id} not present, creating")
    else:
        logger.error(f"Unsupported storage type: {storage}")
        raise ValueError(f"Unsupported storage type: {storage}")


    posting_info = parse.parse_posting_info(response)
    logger.debug(f"Parsed posting_info: {posting_info}")
    posting_links = parse.parse_posting_links(response)
    logger.debug(f"Parsed posting_links: {posting_links}")
    posting = Posting(
        **posting_info,
        org_acronym=org_acronym,
        id=int(posting_id),
        url=response.url,
    )
    posting_links = [
        PostingLink(
            posting_id=posting.id,
            url=link,
            kind=kind,
            last_updated=datetime.datetime.now(datetime.timezone.utc)
        ) for kind, links in posting_links.items()
        for link in links
    ]
    if session is not None:
        session.add_all([posting, *posting_links])
        session.commit()
        logger.info(f"Persisted posting and links for posting_id={posting_id}")
    logger.info(f"Completed fetch_persist_posting for posting_id={posting_id}")
    return posting


def discover_new_postings(n: int = 1, storage: StorageType = StorageType.LOCAL) -> list[Posting]:
    """
    Discover new PLACE public market postings.
    """
    logger.info(f"Starting discover_new_postings with n={n}, storage={storage}")
    if storage == StorageType.LOCAL:
        engine, session = connect_to_db()
        create_tables(engine)
        logger.debug("Connected to DB and ensured tables exist.")
    else:
        logger.error(f"Unsupported storage type: {storage}")
        raise ValueError(f"Unsupported storage type: {storage}")

    new_postings = []

    place_posting_iterator = navigate.PlacePostingIterator()
    logger.info("Initialized PlacePostingIterator.")
    for posting_links in place_posting_iterator.iter_n_batches(n):
        logger.debug(f"Fetched posting_links batch: {posting_links}")
        for link in posting_links:
            try:
                posting_id, org_acronym, response = fetch.fetch_posting_page(link)
                logger.info(f"Fetched posting page for link={link}, posting_id={posting_id}")
                posting = fetch_persist_posting(response, posting_id, org_acronym, storage=storage)
                new_postings.append(posting)
                logger.info(f"Discovered and persisted posting_id={posting_id}")
            except Exception as e:
                logger.error(f"Error processing link={link}: {e}")
                raise e
    logger.info(f"Completed discover_new_postings, found {len(new_postings)} new postings.")
    return new_postings
