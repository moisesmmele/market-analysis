from text_processor import TextProcessor
from collections import Counter
from session import Session
from config import config
from topic import Topic
from typing import Any
import json

class JobspyProcessor:
    session: Session
    topics: list[Topic]
    results: list[dict[str, Any]]

    # Currently only dealing with linkedin data.
    # For future refactors, this should be way more robust
    # We should somehow detect what is the data source
    # prolly hoisting it in Session from meta, or explicitly setting it
    linkedin_levels = {
        "internship": ["internship"],
        "junior": ["entry level"],
        "mid-level": ["associate"],
        "senior": ["mid-senior level"],
        "executive": ["director", "executive"],
        "Not Available": ["not applicable"],
    }

    def __init__(self, session: Session, topics: list[Topic]) -> None:
        self.session = session
        self.topics = topics
        self.results = list()

    def process(self) -> list[Any]:
        self.results = []
        for topic in self.topics:
            self.results.append(self.process_topic(topic))
        return self.results

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
            # again, just to be clear, using only linkedin mappings
            # future refactors would require using dynamic mappings
            for key in self.linkedin_levels.keys():
                if listing.job_level.lower() in self.linkedin_levels.get(key):
                    # bucket is dynamically selected
                    bucket: Counter[any] = buckets[key]["bucket"]
                    # every processed listing is counted
                    buckets[key]["listings"] += 1
                    total["listings"] += 1
                    break

            # Process it
            # Description is explicitly a jobspy field.
            # For future refactors/abstraction, we should make sure
            # Description is either hoisted or mapped to something else
            description = json.loads(listing.raw_data).get("description")
            sanitized = TextProcessor.sanitize(description)
            # extract words and bigrams
            words = sanitized.split()
            bigrams = TextProcessor.extract_bigrams(sanitized)
            #merge the lists and converts to a set for automatic deduplication
            combined = set(words + bigrams)
            # find matches between the list of terms in our loaded topic and our combined set
            counted_words = TextProcessor.find_matches(combined, topic.terms)

            # Update the counters (buckets)
            if bucket is not None:
                bucket.update(counted_words)
            total["bucket"].update(counted_words)

        # create a new dict with matches count filtered by job level
        filtered = dict()
        for key, value in buckets.items():
            filtered[key] = {"listings": value["listings"], "matched": len(value["bucket"]),"counts": dict(value["bucket"])}

        # update the filtered dict to include count of listings per job level
        count_per_level = dict()
        for key, value in filtered.items():
            count_per_level[key] = value["listings"]

        # Build final dict/JSON output
        data.update({"total": {"listings": total["listings"], "per_level": dict(count_per_level), "matched": len(total["bucket"]), "counts": dict(total["bucket"])}})
        data.update({"filtered_by_job_level": filtered})

        return data