from config import config
from topic import Topic
from typing import Any
import json

class TopicLoader:

    _LOADED_TOPICS: dict[int, Topic] | None = None

    @classmethod
    def _ensure_loaded(cls) -> None:
        if not cls._LOADED_TOPICS or config.mode.dev:
            cls.load()

    @classmethod
    def load(cls) -> None:
        cls._LOADED_TOPICS: dict[int, Topic] = {}
        files = sorted(config.topics.glob("*.json"))
        for index, file in enumerate(files):
            with open(file, "r", encoding='utf-8') as f:
                cls._LOADED_TOPICS[index] = Topic(**json.load(f))

    @classmethod
    def get_available(cls) -> dict[int, dict[str, str]]:
        cls._ensure_loaded()
        return {
            index: {
                "title": topic.title,
                "description": topic.description,
                "terms_count": str(len(topic.terms))
            }
            for index, topic in cls._LOADED_TOPICS.items()
        }

    @classmethod
    def select(cls, selected: list[int] = None, all_topics: bool = False ) -> list[Topic]:
        cls._ensure_loaded()

        if all_topics:
            return [cls._LOADED_TOPICS[i] for i in cls._LOADED_TOPICS]

        if not selected:
            return []

        return [
            cls._LOADED_TOPICS[index]
            for index in selected
            if index in cls._LOADED_TOPICS
        ]
