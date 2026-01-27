from dataclasses import dataclass
from datetime import datetime

@dataclass
class Listing:
    location: str
    company: str
    job_level: str
    title: str
    date_posted: datetime
    id: int = None
    raw_data: str = None