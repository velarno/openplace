"""
Workflows for LLM-based entity extraction from tender documents.
"""

import asyncio
import logging
from typing import Optional

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

import openplace.storage.local.queries as q
from openplace.tasks.extract.llm import (
    extract_entities_from_text,
    convert_extraction_to_labels,
)

logger = logging.getLogger(__name__)


async def extract_and_store_labels(
    archive_content_id: int,
    content_text: str,
    model_name: str = "claude-3-5-sonnet-20241022",
    api_key: Optional[str] = None,
) -> int:
    """
    Extract entities from a single archive content and store in database.

    Args:
        archive_content_id: The ID of the archive content.
        content_text: The text content to extract from.
        model_name: The model to use for extraction.
        api_key: Optional API key for the model.

    Returns:
        Number of labels extracted and stored.
    """
    try:
        # Extract entities using LLM
        logger.info(f"Extracting entities from archive content {archive_content_id}")
        extraction = await extract_entities_from_text(
            content_text,
            model_name=model_name,
            api_key=api_key,
        )

        # Convert to label format
        labels = convert_extraction_to_labels(extraction, archive_content_id)

        logger.info(f"Extracted {len(labels)} labels from archive content {archive_content_id}")

        # Store in database
        if labels:
            q.upsert_archive_labels(
                archive_id=archive_content_id,
                label_data=labels,
            )

        # Mark as processed
        q.set_archive_content_inference_done(archive_content_id)

        return len(labels)

    except Exception as e:
        logger.error(f"Error extracting from archive content {archive_content_id}: {e}")
        raise


async def batch_extract_labels(
    limit: int = 10,
    model_name: str = "claude-3-5-sonnet-20241022",
    api_key: Optional[str] = None,
    concurrent: int = 1,
) -> tuple[int, int]:
    """
    Extract labels from multiple unprocessed archive contents.

    Args:
        limit: Maximum number of archive contents to process.
        model_name: The model to use for extraction.
        api_key: Optional API key for the model.
        concurrent: Number of concurrent extractions (be careful with API limits).

    Returns:
        Tuple of (number of archives processed, total labels extracted).
    """
    # Get unprocessed archive contents
    archive_contents = q.get_unprocessed_archive_contents(limit=limit)

    if not archive_contents:
        logger.info("No unprocessed archive contents found")
        return 0, 0

    logger.info(f"Found {len(archive_contents)} unprocessed archive contents")

    total_labels = 0
    processed_count = 0

    # Create progress bar
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
    ) as progress:
        task = progress.add_task(
            f"Extracting entities using {model_name}...",
            total=len(archive_contents)
        )

        # Process in batches based on concurrent setting
        for i in range(0, len(archive_contents), concurrent):
            batch = archive_contents[i:i + concurrent]

            # Create tasks for this batch
            tasks = [
                extract_and_store_labels(
                    ac.id,
                    ac.content,
                    model_name=model_name,
                    api_key=api_key,
                )
                for ac in batch
            ]

            # Run batch concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            for archive_content, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.error(f"Failed to process archive content {archive_content.id}: {result}")
                else:
                    total_labels += result
                    processed_count += 1

                progress.update(task, advance=1)

    logger.info(
        f"Processed {processed_count}/{len(archive_contents)} archive contents, "
        f"extracted {total_labels} total labels"
    )

    return processed_count, total_labels


def run_batch_extraction(
    limit: int = 10,
    model_name: str = "claude-3-5-sonnet-20241022",
    api_key: Optional[str] = None,
    concurrent: int = 1,
) -> tuple[int, int]:
    """
    Synchronous wrapper for batch_extract_labels.

    Args:
        limit: Maximum number of archive contents to process.
        model_name: The model to use for extraction.
        api_key: Optional API key for the model.
        concurrent: Number of concurrent extractions.

    Returns:
        Tuple of (number of archives processed, total labels extracted).
    """
    return asyncio.run(
        batch_extract_labels(
            limit=limit,
            model_name=model_name,
            api_key=api_key,
            concurrent=concurrent,
        )
    )
