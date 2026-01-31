from dataclasses import dataclass

@dataclass
class Listing:
    id: int = None
    session_id: str = None
    raw_data: str = None

    @classmethod
    def from_row(cls, row: dict) -> 'Listing':
        return cls(**{key: row[key] for key in cls.__annotations__.keys()})
