from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class MovieDTO:
    date: datetime
    moviecd: str
    movienm: str
    showcnt: int
    scrncnt: int
    opendt: str
    audiacc: int
    repgenrenm: str

