from dataclasses import dataclass
from datetime import datetime
from typing import Any
import json

from app.entities.listing import Listing
from app.enums import SubsessionStatus

@dataclass
class Subsession:
    id: int = None
    session_id: int = None
    search_term: str = None
    status: SubsessionStatus = None
    start_date: datetime = None
    finish_date: datetime = None
    listings: dict[int, Listing] = None

    def start(self):
        self.start_date = datetime.now()
        self.status = SubsessionStatus.pending

    def finish(self):
        self.finish_date = datetime.now()
        self.status = SubsessionStatus.done

    def append_listing(self, listing: Listing):
        listing.session_id = self.session_id
        if self.listings is None:
            self.listings = {}
        if not listing.id:
            index = len(self.listings) + 1
            self.listings[index] = listing
        else:
            self.listings[listing.id] = listing

    @classmethod
    def from_row(cls, data: dict[str, str|Any]) -> 'Subsession':

        _data = dict(data).copy()

        if 'start_date' in _data and _data['start_date']:
            _data['start_date'] = datetime.fromisoformat(_data.get('start_date'))

        if 'finish_date' in _data and _data['finish_date']:
            _data['finish_date'] = datetime.fromisoformat(_data.get('finish_date'))

        if 'subsession_status' in _data and _data['subsession_status']:
            status: str = _data['subsession_status']
            _data['subsession_status'] = SubsessionStatus(status)

        valid_keys = cls.__annotations__.keys()
        return cls(**{k: v for k, v in _data.items() if k in valid_keys})


