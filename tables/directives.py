from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Directive:

    name: str
    lines: List[str]
    origin: Optional[str] = None

    def __repr__(self):
        return f"***{self.name}\n" + "\n".join(self.lines)


