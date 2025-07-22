import re
from enum import Enum

class FileExtensionPattern(Enum):
    """Patterns linked to different file extensions."""
    PDF = re.compile(r'\.pdf$', re.IGNORECASE)
    DOCX = re.compile(r'\.docx$', re.IGNORECASE)
    DOC = re.compile(r'\.doc$', re.IGNORECASE)
    XLSX = re.compile(r'\.xlsx$', re.IGNORECASE)
    XLS = re.compile(r'\.xls$', re.IGNORECASE)
    CSV = re.compile(r'\.csv$', re.IGNORECASE)
    TXT = re.compile(r'\.txt$', re.IGNORECASE)
    ZIP = re.compile(r'\.zip$', re.IGNORECASE)
    RAR = re.compile(r'\.rar$', re.IGNORECASE)
    GZ = re.compile(r'\.gz$', re.IGNORECASE)
    BZ2 = re.compile(r'\.bz2$', re.IGNORECASE)
    XZ = re.compile(r'\.xz$', re.IGNORECASE)
    LZMA = re.compile(r'\.lzma$', re.IGNORECASE)
    LZ = re.compile(r'\.lz$', re.IGNORECASE)
    PARQUET = re.compile(r'\.parquet$', re.IGNORECASE)

    def __call__(self, text: str) -> bool:
        return self.value.match(text) is not None

class FileStructurePattern(Enum):
    """Patterns linked to different file headings."""
    ANNEX = re.compile(r'\*?\*?ANNEXE ?[0-9]+.*\*?\*?$', re.IGNORECASE)
    FILE_DECLARATION = re.compile(r'## File:.*$')

    HEADER_NUMBERED = re.compile(r'^([A-Z0-9]{1,2}[\.\-\)]){1,5}\w*$', re.IGNORECASE)
    """Matches patterns like: I.1.1) , A.2.1, 1-1-1, etc."""
    HEADER_LIT_FR = re.compile(r'^Section [A-Z]+:.*$', re.IGNORECASE)
    """Matches patterns like: Section A: <text> , Section B: <text> , etc."""

    EMPTY_LINE_OR_DELIMITER = re.compile(r'^[\w\-•o]{1,3}$', re.IGNORECASE)

    FOOTER_LIT_PAGE_COUNT_EN = re.compile(r'Page [0-9]+ of [0-9]+$', re.IGNORECASE)
    FOOTER_LIT_PAGE_COUNT_FR = re.compile(r'Page [0-9]+ sur [0-9]+$', re.IGNORECASE)
    
    FOOTER_NUM_PAGE_COUNT = re.compile(r'[0-9]+ [\-\|] [0-9]+$', re.IGNORECASE)
    

