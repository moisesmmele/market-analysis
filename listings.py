import json
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Listing:
    id: int
    session_id: int
    location: str
    company: str
    job_level: str
    title: str
    date_posted: datetime
    raw_data: json