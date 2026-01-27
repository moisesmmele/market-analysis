from datetime import datetime

class Listing:
    id: int
    session_id: int
    location: str
    company: str
    job_level: str
    title: str
    date_posted: datetime
    raw_data: str