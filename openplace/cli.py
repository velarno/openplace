from openplace.workflows.metadata import discover_new_postings
from openplace.workflows.files import download_pending_files
from openplace.tasks.export.archives import export_archives as export_archives_task

import openplace.storage.local.queries as q

import logging
import typer
from typer import Option, Argument
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
    storage: StorageType = Option(StorageType.LOCAL, "--storage", "-s", help="Storage type"),
    limit: int = Option(100, "--limit", "-l", help="Maximum number of postings to return"),
):
    """
    List postings.
    """
    if storage == StorageType.LOCAL:
        postings = q.list_postings(storage=storage, limit=limit)
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
    debug: bool = Option(False, "--debug", "-d", help="Debug mode", show_default=True),
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
def export_archives(
    output_dir: str = Option(".", "--output-dir", "-o", help="Output directory", show_default=True),
    output_format: str = Option("parquet", "--output-format", "-f", help="Output format", show_default=True),
    debug: bool = Option(False, "--debug", "-d", help="Debug mode", show_default=True),
):
    """
    Export archives to a file.
    If no output directory is provided, the archives will be exported to a file named "archives-<date>.parquet" in the current directory.
    If no output format is provided, the archives will be exported to a parquet file.
    """
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    export_archives_task(output_dir=output_dir, output_format=output_format)

def main():
    app()

if __name__ == "__main__":
    typer.run(main)