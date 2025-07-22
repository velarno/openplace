from dataclasses import dataclass, asdict
from unidecode import unidecode
from typing import Type

@dataclass
class DateContent:
    """
    A dataclass representing the piece of content which contains a date (within a document).
    """
    content: str
    index: int
    length: int

    def __str__(self) -> str:
        return unidecode(self.content)
    
    def __repr__(self) -> str:
        return f"DateContent(content={self.content}, index={self.index}, length={self.length})"
    
    def to_json(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_json(cls: Type["DateContent"], data: dict) -> "DateContent":
        return cls(
            content=unidecode(data["content"]),
            index=data["index"],
            length=data["length"],
        )

@dataclass
class DateContext:
    """
    A dataclass representing date information and its surrounding context (within a document).
    This essentially stores the N-k and N+k lines around the N-th line of the document if N matches a date pattern.
    """
    content: str
    index: int
    before: str
    after: str
    window_size: int

    def __str__(self) -> str:
        return unidecode(self.content)
    
    def __repr__(self) -> str:
        return f"DateContext(content={self.content}, index={self.index}, before={self.before}, after={self.after})"

    def to_json(self) -> dict:
        return {
            "content": self.content,
            "index": self.index,
            "before": self.before,
            "after": self.after,
            "window_size": self.window_size,
        }
    
    @classmethod
    def from_json(cls: Type["DateContext"], data: dict) -> "DateContext":
        return cls(
            content=unidecode(data["content"]),
            index=data["index"],
            before=unidecode(data["before"]),
            after=unidecode(data["after"]),
            window_size=data["window_size"],
        )