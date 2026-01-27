from text_processor import TextProcessor
from collections import Counter
from database import Database
from session import Session
from config import config
from topic import Topic
from typing import Any
import json

class JobspyProcessor:
    session: Session
    topics: list[Topic]
    results: list[dict[str, Any]]
    linkedin_levels = {
        "internship": ["internship"],
        "junior": ["entry level"],
        "mid-level": ["associate"],
        "senior": ["mid-senior level"],
        "executive": ["director", "executive"],
        "Not Available": ["not applicable"],
    }

    def __init__(self, session: Session) -> None:
        self.session = session
        self.topics = self.load_topics()
        self.results = list()

    def process(self) -> list[Any]:
        self.results = []
        for topic in self.topics:
            self.results.append(self.process_topic(topic))
        return self.results

    @staticmethod
    def load_topics() -> list[Topic]:
        topics = list()
        for file in config.topics.glob("*.json"):
            with open(file, "r", encoding='utf-8') as f:
                topics.append(Topic(**json.load(f)))
        return topics

    def process_topic(self, topic: Topic) -> dict[str, dict[str, int] | str]:
        data = {"topic": topic.title, "description": topic.description}
        total = dict({"listings": 0, "bucket": Counter()})
        buckets = dict()

        # Generate level buckets
        for level in self.linkedin_levels:
            buckets[level] = dict({"listings": 0, "bucket": Counter()})

        # Define which bucket this listing belongs to
        for listing in self.session.listings:
            bucket = None
            for key in self.linkedin_levels.keys():
                if listing.job_level.lower() in self.linkedin_levels.get(key):
                    bucket: Counter[any] = buckets[key]["bucket"]
                    buckets[key]["listings"] += 1
                    total["listings"] += 1
                    break

            # Process it
            description = json.loads(listing.raw_data).get("description")
            sanitized = TextProcessor.sanitize(description)
            words = sanitized.split()
            bigrams = TextProcessor.extract_bigrams(sanitized)
            words.extend(bigrams)
            counted_words = TextProcessor.find_matches(set(words), topic.terms)

            # Update buckets
            if bucket is not None:
                bucket.update(counted_words)
            total["bucket"].update(counted_words)

        # Build final json output
        filtered = dict()
        for key, value in buckets.items():
            filtered[key] = {"listings": value["listings"], "counts": dict(value["bucket"])}

        data.update({"total": {"listings": total["listings"], "counts": dict(total["bucket"])}})
        data.update({"filtered_by_job_level": filtered})

        return data