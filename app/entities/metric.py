from datetime import datetime, timedelta
from typing import Any

from app.enums import InfoType

class Metric:
    _context: str
    _start_time: datetime
    _finish_time: datetime
    _duration: timedelta
    _status: bool
    _info: dict[str, Any]

    def __init__(self, context: str):
        self._context = context
        self._info = dict()
        self._start()

    def _start(self) -> datetime:
        self._start_time = datetime.now()
        return self._start_time

    def _finish(self) -> datetime:
        self._finish_time = datetime.now()
        delta = self._finish_time - self._start_time
        self._duration = delta
        return self._finish_time

    def get_context(self) -> str:
        return self._context

    def append_info(
            self, key: int|str, message: Any,
            info_type: InfoType = InfoType.DEFAULT
    ) -> 'Metric':

        if info_type == InfoType.DEFAULT:
            if "info" not in self._info:
                self._info["info"] = {}
            self._info["info"][key] = message

        elif info_type == InfoType.WARNING:
            if "warning" not in self._info:
                self._info["warning"] = {}

            meta = self._info["warning"]
            index = len(meta) + 1
            meta[index] = {key: message}

        elif info_type == InfoType.META:
            if "meta" not in self._info:
                self._info["meta"] = {}

            meta = self._info["meta"]
            index = len(meta) + 1
            meta[index] = {key: message}

        return self

    def success(self) -> 'Metric':
        self._finish()
        self._status = True
        return self

    def failure(self) -> 'Metric':
        self._finish()
        self._status = False
        return self

    def to_dict(self):
        data: dict[str, Any] = {
            "context": self._context,
            "start": self._start_time.isoformat(),
            "finish": self._finish_time.isoformat(),
            "duration": str(self._duration),
            "status": self._status,
        }
        if self._info:
            data["meta"] = self._info
        return data