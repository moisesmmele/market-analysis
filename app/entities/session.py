from dataclasses import dataclass
from datetime import datetime
from typing import Any
import json

from app.entities.subsession import Subsession
from enums import SubsessionStatus


@dataclass
class Session:
    id: int = None
    title: str = None
    description: str = None
    location: str = None
    date: datetime = None
    finish_date: datetime = None
    subsessions: dict[int, Subsession] = None
    provider: str = None
    platform: str = None
    meta: dict[str, Any] = None

    def start(self):
        self.date = datetime.now()

    def add_subsession(self, search_term: str):
        if not self.subsessions:
            self.subsessions = {}
        subsession = Subsession()
        subsession.session_id = self.id
        subsession.search_term = search_term
        subsession.start()
        index = len(self.subsessions) + 1
        self.subsessions[index] = subsession

    def resolve_finished(self):
        if not self.subsessions: return

        dates: list[datetime] = []
        for subs in self.subsessions.values():
            if subs.status != SubsessionStatus.done: return
            dates.append(subs.finish_date)

        self.finish_date = max(dates)

    def __post_init__(self):
        self.resolve_finished()

    @classmethod
    def from_row(cls, data: dict[str, str|Any], subsessions: dict[int, Subsession]) -> 'Session':

        # data shallow copy so we don't mutate original dict
        _data = data.copy()

        # -- Mutations --
        if 'date' in _data and _data['date']:
            _data['date'] = datetime.fromisoformat(_data.get('date'))

        if 'meta' in _data and _data['meta']:
            json_str = _data['meta']
            _data['meta'] = json.loads(json_str)

        valid_keys = cls.__annotations__.keys()
        session =  cls(**{k: v for k, v in _data.items() if k in valid_keys})
        session.subsessions = subsessions
        session.resolve_finished()
        return session
