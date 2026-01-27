from dataclasses import dataclass
from typing import Dict, Any, List
from datetime import datetime
import json

from listing import Listing

class Session:
    id: int
    title: str
    description: str = "THIS IS A TEST LOREM IPSUM DOLOR SIT AMET CONSECTETUR ADIPISCING ELIT"
    start_time: datetime
    finish_time: datetime
    listings: List[Listing]
    meta: Dict[Any, Any]

    def __init__(self, title: str):
        self.title = title
        
    def start(self):
        self.start_time = datetime.now()

    def finish(self):
        self.finish_time = datetime.now()