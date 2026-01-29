from dataclasses import dataclass

@dataclass
class Listing:
    id: str
    external_id: str = None
    title: str = None
    description: str = None
    job_level: str = None