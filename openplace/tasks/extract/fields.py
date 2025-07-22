import json

import openplace.storage.local.queries as q

from openplace.tasks.extract.utils import context_relevant_lines, DateContext
from openplace.storage.local.models import ArchiveContent


def extract_date_information(item: ArchiveContent | str, window_size: int = 9):
    """
    Extract the date information from the given item.
    """
    for date_context in context_relevant_lines(item, window_size, with_context=True):
        if isinstance(date_context, DateContext):
            print(json.dumps(date_context.to_json()))
        # TODO: implement LLM based extraction with at least local + cloud LLMs

def clean_content(content: str) -> str:
    """
    Clean the content of the given item.
    """
    return "\n".join(
        line for line in context_relevant_lines(
            content, with_context=False
            ) 
        if isinstance(line, str)
        )


if __name__ == "__main__":
    for archive_contents in q.paginate_archive_contents(batch_size=10):
        for archive_content in archive_contents:
            # extract_date_information(content)
            # print(clean_content(archive_content.content))
            print(archive_content.content)
            break