from typing import Iterable, Callable
from unidecode import unidecode
import re
from openplace.storage.local.models import ArchiveContent

Cleaner = Callable[[str], str]
"""Type alias for a function that cleans a string."""

def normalize_text(text: str) -> str:
    """
    Normalize the given text by removing newlines and unidecoding.
    """
    text_no_dots = re.sub(r'\.\.+', '.', text)
    return unidecode(text_no_dots.replace("\n", " "))


def heading_cleaner(text: str) -> str:
    pass

def clean_each_paragraph(
    item: ArchiveContent | str,
    cleaner: Cleaner,
    ) -> Iterable[str]:
    """
    Clean each paragraph of the given item.

    Args:
        item (ArchiveContent | str): The item to clean.
        cleaner (Cleaner): The cleaner function to use.
    """
    content = (
        item.content if isinstance(item, ArchiveContent) 
        else item if isinstance(item, str) else None
    )

    if content is None:
        raise ValueError(f"Invalid item type: {type(item)}")
    
    lines = content.split("\n\n") # prefer splitting by paragraphs (= \n\n)
    for line in lines:
        normalized_line = normalize_text(line)
        yield cleaner(normalized_line)

def clean_content(
    item: ArchiveContent | str,
    cleaner: Cleaner,
    ) -> str:
    """
    Clean the content of the given item.
    """
    return "\n".join(clean_each_paragraph(item, cleaner))