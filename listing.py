from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Listing:
    location: str
    company: str
    job_level: str
    title: str
    date_posted: datetime
    session_id: Optional[int] = None
    id: Optional[str] = None
    raw_data: Optional[str] = None