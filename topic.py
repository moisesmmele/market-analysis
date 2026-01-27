from dataclasses import dataclass

@dataclass
class Topic:
    title: str
    description: str
    terms: dict[str, list[str]]