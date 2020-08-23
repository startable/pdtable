from dataclasses import dataclass
from typing import List, Optional


class MetadataBlock(dict):

    def __init__(self, origin: Optional[str] = None):
        super().__init__()
        self.origin = origin

    def __repr__(self):
        return "\n".join(f"{k}: {self[k]}" for k in self)


@dataclass
class Directive:

    name: str
    lines: List[str]
    origin: Optional[str] = None

    def __repr__(self):
        return f"***{self.name}\n" + "\n".join(self.lines)


