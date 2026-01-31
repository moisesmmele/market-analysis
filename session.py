from dataclasses import dataclass
from datetime import datetime
from typing import Any
import json

from listing import Listing


@dataclass
class Session:
    title: str = None
    description: str = None
    start_time: datetime = None
    finish_time: datetime = None
    listings: dict[int, Listing] = None
    meta: dict[str, Any] = None
    id: int = None

    @classmethod
    def from_row(cls, data: dict[str, str|Any]) -> 'Session':

        # data shallow copy so we don't mutate original dict
        _data = data.copy()

        # -- Mutations --
        if 'datetime_start' in _data and _data['datetime_start']:
            _data['start_time'] = datetime.fromisoformat(_data.get('datetime_start'))

        if 'datetime_finish' in _data and _data['datetime_finish']:
            _data['finish_time'] = datetime.fromisoformat(_data.get('datetime_finish'))

        if 'meta' in _data and _data['meta']:
            json_str = _data['meta']
            _data['meta'] = json.loads(json_str)

        valid_keys = cls.__annotations__.keys()
        return cls(**{k: v for k, v in _data.items() if k in valid_keys})


    def start(self):
        self.start_time = datetime.now()

    def finish(self):
        self.finish_time = datetime.now()