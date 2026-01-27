from config import config
from topic import Topic
import json

class TopicLoader:
    topics: dict[str, Topic]

    def get_available(self) -> dict[str, Topic]:
        topics: dict[str, Topic] = dict()
        available: set[str] = set()
        for file in config.topics.glob("*.json"):
            with open(file, "r", encoding='utf-8') as f:
                topic = Topic(**json.load(f))
                topics[topic.title] = topic
                available.add(topic.title)
        self.topics = topics
        return available

    def load(self, selected: set[str]) -> list[Topic]:
        topics: list[Topic] = list()
        for topic in selected:
            selected_topic = self.topics[topic]
            topics.append(selected_topic)
        return topics