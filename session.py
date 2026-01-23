from dataclasses import dataclass
from typing import Dict, Any, List
from datetime import datetime
import json

from listings import Listing

@dataclass
class Session:
    id: int
    title: str
    start_time: datetime
    finish_time: datetime
    listings: List[Listing]
    meta: Dict[str, Any]

    def __init__(self, title: str):
        self.title = title
        
    def start(self):
        self.start_time = datetime.now()

    def finish(self):
        self.finish_time = datetime.now()

    def metaToJson(self):
        return json.dumps(self.meta, default=str)