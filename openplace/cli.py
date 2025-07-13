from openplace.workflows.metadata import discover_new_postings, fetch_posting_files
from openplace.storage.local.queries import get_posting_links, list_postings as list_postings_local
from openplace.storage.local.queries import remove_posting as remove_posting_local

import logging
import typer
from typer import Option, Argument
from openplace.tasks.store.types import StorageType

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
        postings = list_postings_local(storage=storage, limit=limit)
        for posting in postings:
            typer.echo(f"{posting.id} {posting.title} {posting.fetching_status} {posting.last_updated}")
    else:
        raise ValueError(f"Storage type {storage} not supported")

@app.command()
def list_links(
    posting_id: int = Option(..., "--posting-id", "-p", help="Posting ID"),
    storage: StorageType = Option(StorageType.LOCAL, "--storage", "-s", help="Storage type"),
    ):
    """"""
    if storage == StorageType.LOCAL:
        links = get_posting_links(posting_id)

    else:
        raise ValueError(f"Storage type {storage} not supported")

    for link in links:
        typer.echo(link)

@app.command()
def remove_posting(
    posting_id: int = Option(..., "--posting-id", "-p", help="Posting ID"),
    storage: StorageType = Option(StorageType.LOCAL, "--storage", "-s", help="Storage type"),
    ):
    """
    Remove a posting.
    """
    if storage == StorageType.LOCAL:
        remove_posting_local(posting_id)
    else:
        raise ValueError(f"Storage type {storage} not supported")

def main():
    app()

if __name__ == "__main__":
    typer.run(main)