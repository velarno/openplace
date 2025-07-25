import re
from typing import Iterable
from dateutil.parser import parse
from unidecode import unidecode
from openplace.storage.local.models import ArchiveContent
from openplace.tasks.extract.types import DateContext

SINGLE_NUMBER_REGEX = r'^\d+$'
DATE_REGEX = r'(\d+)[\/\-\.](\d+)[\/\-\.](\d+)'
LETTER_SECTION_REGEX = r'^(?:[A-Z]+[\.\-]?)*[A-Z]+(?:[\.\-]?\d+)*[\)\.]?$'
SUBSECTION_REGEX = r'^(?:\d+[\.\-]?)*\d+[\)\.]? \w+$'
SECTION_LITERAL_REGEX = r'^Section *([A-Z0-9]+)[\.\-\:]? *(\w+)\)?$'
PAGE_COUNT_REGEX = r'^(\d+) *[\/\-\|] *(\d+)'
PAGE_LITERAL_COUNT_REGEX = r'^[Pp][Aa][Gg][Ee] *(\d+) [Ss][Uu][Rr] *(\d+)'
IDENTIFIER_REGEX = r'^[A-Z0-9]+[\_\-]?[A-Z0-9]+$'

def is_zip_file_header(text: str) -> bool:
    """
    Check if the given text is a zip file header.
    """
    return (
        text.startswith("## File: ") or
        text.startswith("Content from the zip file")
    )

def is_section(text: str) -> bool:
    """
    Check if the given text is a section.
    """
    return (
        re.match(LETTER_SECTION_REGEX, text) is not None or
        re.match(SUBSECTION_REGEX, text) is not None or
        re.match(SECTION_LITERAL_REGEX, text) is not None
    )

def is_identifier(text: str) -> bool:
    """
    Check if the given text is an identifier.
    """
    return re.match(IDENTIFIER_REGEX, text) is not None

def is_page_count(text: str) -> bool:
    """
    Check if the given text is a page count.
    """
    return (
        re.match(PAGE_COUNT_REGEX, text) is not None or
        re.match(PAGE_LITERAL_COUNT_REGEX, text) is not None
    )

def is_single_number(text: str) -> bool:
    """
    Check if the given text is a single number.
    """
    return re.match(SINGLE_NUMBER_REGEX, text) is not None

def clean_text(text: str) -> str:
    """
    Clean the given text by removing newlines and unidecoding.
    """
    text_no_dots = re.sub(r'\.\.+', '.', text)
    return unidecode(text_no_dots.replace("\n", " "))
    
def clean_context(context: list[str]) -> str:
    """
    Clean the given context by removing newlines and unidecoding.
    """
    return clean_text("\n".join(context))

def word_has_date(text: str) -> bool:
    """
    Check if the given text contains a date matching the DATE_REGEX pattern.

    Args:
        text (str): The text to search for a date.

    Returns:
        bool: True if a date is found, False otherwise.
    """
    try:
        parse(text, fuzzy=True)
        return True
    except Exception:
        return False

def str_has_date(text: str) -> bool:
    """
    Check if the given text contains a date matching the DATE_REGEX pattern.
    """
    if any(
        [
            is_section(text),
            is_page_count(text),
            is_single_number(text),
            is_zip_file_header(text),
            is_identifier(text),
        ]):
        return False
    return any(word_has_date(word) for word in text.split())

def archive_content_has_date(archive: ArchiveContent) -> bool:
    """
    Check if the content of the given ArchiveContent contains a date.

    Args:
        archive (ArchiveContent): The archive content to check.

    Returns:
        bool: True if a date is found in the content, False otherwise.
    """
    return str_has_date(archive.content)

def has_date(item: ArchiveContent | str) -> bool:
    """
    Check if the given item contains a date.

    Args:
        item (ArchiveContent | str): The item to check.

    Returns:
        bool: True if a date is found in the item, False otherwise.
    """
    if isinstance(item, ArchiveContent):
        return archive_content_has_date(item)
    elif isinstance(item, str):
        return str_has_date(item)
    else:
        raise ValueError(f"Invalid item type: {type(item)}")

def context_relevant_lines(
    item: ArchiveContent | str, window_size: int = 5,
    with_context: bool = True,
    ) -> Iterable[DateContext | str]:
    """
    Retrieve the date with its surrounding context from the given item.

    Args:
        item (ArchiveContent | str): The item to retrieve the date from.
        window_size (int): The number of lines to retrieve before and after the date.
    """
    num_context_lines = (window_size - 1) // 2 # half before, half after
    content = (
        item.content if isinstance(item, ArchiveContent) 
        else item if isinstance(item, str) else None
    )

    if content is None:
        raise ValueError(f"Invalid item type: {type(item)}")
    
    lines = content.split("\n\n") # prefer splitting by paragraphs (= \n\n)
    for i, line in enumerate(lines):
        clean_line = clean_text(line)
        if str_has_date(clean_line):
            if with_context:
                before = clean_context(lines[max(0, i - num_context_lines):i]) # N-k lines
                after = clean_context(lines[i + 1:min(len(lines), i + num_context_lines + 1)]) # N+k lines
                yield DateContext.from_json(
                    {
                        "content": clean_line,
                        "index": i,
                        "before": before,
                        "after": after,
                        "window_size": window_size,
                    }
                )
            else:
                yield clean_line