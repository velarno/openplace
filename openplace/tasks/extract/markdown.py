import os
from markitdown import MarkItDown
from pathlib import Path
import asyncio
import logging
from openplace.storage.local import queries as q
from openplace.tasks.store.writers import parse_archive_name
from openplace.tasks.store.types import ArchiveContent

md = MarkItDown()

logger = logging.getLogger(__name__)

def find_archive_paths(directory: str) -> list[Path]:
    """
    Find all archive paths in the given directory.
    """
    return list(Path(directory).glob('*.zip'))

async def extract_markdown(archive_path: Path, persist: bool = True) -> ArchiveContent | None:
    """
    Extract the markdown from the given archive path and record the content in the database.
    """
    logger.info(f"Extracting markdown from {archive_path}")
    posting_id, filename, file_type = parse_archive_name(archive_path.name)
    archive_content = md.convert(archive_path).markdown

    if persist:
        q.record_archive_content(
            path=archive_path.name,
            content=archive_content,
            posting_id=posting_id,
        )
    return ArchiveContent(
        posting_id=posting_id,
        filename=filename,
        file_type=file_type,
        content=archive_content,
    )

async def extract_all_archives(directory: str) -> list[ArchiveContent]:
    """
    Extract the markdown from all archives in the given directory.
    """
    # TODO: test async extraction performance
    archive_paths = find_archive_paths(directory)

    futures = [asyncio.create_task(extract_markdown(archive_path)) for archive_path in archive_paths]
    return [result for result in await asyncio.gather(*futures) if result is not None]

def extract_all_archives_concurrently(directory: str) -> list[ArchiveContent]:
    """
    Extract the markdown from all archives in the given directory.
    """
    return asyncio.run(extract_all_archives(directory))