from config import config
from topic import Topic
import json

class TopicLoader:
    topics: dict[str, Topic]
    def __init__(self):
        self.topics: dict[str, Topic] = {}

    def get_available(self) -> set[str]:
        """read JSON files with topic definitions and returns the available topics"""
        topics: dict[str, Topic] = {}
        available: set[str] = set()
        for file in config.topics.glob("*.json"):
            try:
                with open(file, "r", encoding='utf-8') as f:
                    topic = Topic(**json.load(f))
                    topics[topic.title] = topic
                    available.add(topic.title)
            except (json.decoder.JSONDecodeError, TypeError) as e:
                print(f"error: could not parse {file}\n{str(e)}")
        self.topics = topics
        return available

    def load(self, selected: set[str]) -> list[Topic]:
        topics: list[Topic] = list()
        for topic in selected:
            selected_topic = self.topics[topic]
            topics.append(selected_topic)
        return topics