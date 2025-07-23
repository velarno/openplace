import os

from openplace.workflows.metadata import discover_new_postings
from openplace.workflows.files import download_pending_files
from openplace.tasks.export.archives import export_archives as export_archives_task
from openplace.tasks.extract.markdown import extract_all_archives_concurrently

import openplace.storage.local.queries as q

import logging
import typer

from pathlib import Path
from typer import Option
from openplace.tasks.store.types import StorageType


logger = logging.getLogger(__name__)

app = typer.Typer(
    name="openplace",
    add_completion=False,
    no_args_is_help=True,
    add_help_option=True,
    help="""
    OpenPlace CLI, a tool to discover and fetch PLACE public market postings.
    """,
    epilog="""
    Examples:
    $ openplace discover-new-postings --n 10
    $ openplace fetch-posting-files --posting-id 123
    """
)

@app.command()
def discover(
    n: int = Option(1, "--num-postings", "-n", help="Number of postings to discover"),
    storage: StorageType = Option(StorageType.LOCAL, "--storage", "-s", help="Storage type"),
    ):
    """
    Discover new PLACE public market postings.
    """
    discover_new_postings(n, storage)

@app.command()
def list_postings(
    storage: StorageType = Option(StorageType.LOCAL, "--storage", "-S", help="Storage type"),
    limit: int = Option(100, "--limit", "-l", help="Maximum number of postings to return"),
    status: str = Option(None, "--status", "-s", help="Status of the postings"),
):
    """
    List postings.
    """

    if storage == StorageType.LOCAL:
        postings = q.list_postings(storage=storage, limit=limit, status=status)
        if postings:
            typer.echo("id|org_acronym|organization|title|fetching_status|last_updated")
            for posting in postings:
                typer.echo(f"{posting.id}|{posting.org_acronym}|{posting.organization}|{posting.title}|{posting.fetching_status}|{posting.last_updated}")
    else:
        raise ValueError(f"Storage type {storage} not supported")

@app.command()
def list_links(
    posting_id: int = Option(..., "--posting-id", "-i", help="Posting ID"),
    storage: StorageType = Option(StorageType.LOCAL, "--storage", "-S", help="Storage type"),
    ):
    """"""
    if storage == StorageType.LOCAL:
        links = q.get_posting_links(posting_id)

    else:
        raise ValueError(f"Storage type {storage} not supported")

    for link in links:
        typer.echo(link)

@app.command()
def remove_posting(
    posting_id: int = Option(..., "--posting-id", "-i", help="Posting ID"),
    storage: StorageType = Option(StorageType.LOCAL, "--storage", "-S", help="Storage type"),
    ):
    """
    Remove a posting.
    """
    if storage == StorageType.LOCAL:
        q.remove_posting(posting_id)
    else:
        raise ValueError(f"Storage type {storage} not supported")

@app.command()
def fetch_archives(
    storage: StorageType = Option(StorageType.LOCAL, "--storage", "-S", help="Storage type", show_default=True),
    display_progress: bool = Option(True, "--silent", "-s", help="Display progress", show_default=True),
    debug: bool = Option(False, "--debug", "-D", help="Debug mode", show_default=True),
):
    """
    Download pending files.
    """
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    if storage == StorageType.LOCAL:
        download_pending_files(storage=storage, display_progress=display_progress)
    else:
        raise ValueError(f"Storage type {storage} not supported")

@app.command()
def extract_markdown(
    directory: str = Option(os.getcwd(), "--directory", "-d", help="Directory to extract markdown from (contains zip files). Defaults to current working directory.", show_default=False),
    debug: bool = Option(False, "--debug", "-D", help="Debug mode", show_default=True),
):
    """
    Extract markdown from archives.
    """
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    extract_all_archives_concurrently(directory)

@app.command()
def export_archives(
    output_dir: str = Option(".", "--output-dir", "-o", help="Output directory", show_default=True),
    output_format: str = Option("parquet", "--output-format", "-f", help="Output format", show_default=True),
    compression: str = Option("gzip", "--compression", "-C", help="Compression method for jsonl and csv", show_default=True),
    filename_date: bool = Option(False, "--filename-date", "-D", help="Include date in filename (output files will be named archives-<date>.parquet, archives-<date>.jsonl.gz, etc.)", show_default=True),
    debug: bool = Option(False, "--debug", "-D", help="Debug mode", show_default=True),
):
    """
    Export archives to a file.
    If no output directory is provided, the archives will be exported to a file named "archives-<date>.parquet" in the current directory.
    If no output format is provided, the archives will be exported to a parquet file.
    """
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    export_archives_task(output_dir=output_dir, output_format=output_format, compression=compression, use_date_in_filename=filename_date)


@app.command()
def export_archive_content(
    archive_content_id: int = Option(..., "--archive-content-id", "-i", help="Archive content ID"),
    storage: StorageType = Option(StorageType.LOCAL, "--storage", "-S", help="Storage type", show_default=True),
    output_file: str = Option(None, "--output-file", "-o", help="Output file name, else will name it archive_content_<id>.txt unless --terminal is used"),
    to_terminal: bool = Option(False, "--terminal", "-t", help="Output to terminal", show_default=True),
    ):
    """
    Export an archive content to a file.
    """
    if storage == StorageType.LOCAL:
        archive_content = q.get_archive_content_by_id(archive_content_id)
        if archive_content is None:
            typer.echo(f"Archive content with id {archive_content_id} not found")
            return
        if archive_content_id != archive_content.id:
            typer.echo(f"Archive content id mismatch: {archive_content_id} != {archive_content.id}")
            return
        if to_terminal:
            typer.echo(archive_content.content)
        else:
            output_path = Path(output_file) or Path(f"archive_content_{archive_content_id}.txt")
            output_path.write_text(archive_content.content)
            typer.echo(f"Archive content exported to {output_path}")
    else:
        raise ValueError(f"Storage type {storage} not supported")

@app.command()
def bulk_export_archive_contents(
    storage: StorageType = Option(StorageType.LOCAL, "--storage", "-S", help="Storage type", show_default=True),
    limit: int = Option(100, "--limit", "-l", help="Limit the number of archive contents to list", show_default=True),
    output_dir: str = Option(os.getcwd(), "--output-dir", "-o", help="Output directory, defaults to current working directory", show_default=True),
    silent: bool = Option(False, "--silent", "-s", help="Silent mode", show_default=True),
):
    """
    List unprocessed archive contents.
    """
    if storage == StorageType.LOCAL:
        archive_contents = q.get_unprocessed_archive_contents(limit=limit)
        if not silent:
            typer.echo(f"Found {len(archive_contents)} unprocessed archive contents")
        for archive_content in archive_contents:
            output_path = Path(output_dir) / f"{archive_content.id}.txt"
            output_path.write_text(archive_content.content)
            if not silent:
                typer.echo(f"Archive content exported to {output_path}")
    else:
        raise ValueError(f"Storage type {storage} not supported")

def main():
    app()

if __name__ == "__main__":
    typer.run(main)