from PIL.ImageChops import difference

from dynamic_listing_factory import DynamicListingFactory
from dynamic_listing import DynamicListing
from mappings_loader import MappingsLoader
from text_processor import TextProcessor
from collections import Counter
from datetime import datetime
from listing import Listing
from session import Session
from config import config
from topic import Topic
from typing import Any
import json

class Processor:

    _indexed_ngrams = None
    _indexed_job_level: dict[str, list] = None
    _buckets: dict[str, dict[str, Any]] = None
    _listings: dict[int, DynamicListing] = None
    _topics: list[Topic] = None
    _session: Session = None

    results: list[dict[str, Any]] = None
    metrics: dict[str, dict[str, str]] = None

    # TODO: 1. IMPLEMENT BOOL OR METRICS RETURNS FOR EVERY METHOD THAT RETURNS NONE / HAS EFFECT / CHANGES STATE
    # TODO: 2. IMPLEMENT GUARD CLAUSES / EARLY RETURN ON PROCESS METHOD

    def __init__(self, session: Session, topics: list[Topic]) -> None:
        self._session = session
        self.topics = topics

    def process(self) -> Any:
        start = datetime.now()
        self.build_listings()
        self.sanitize_listings()
        self.extract_ngrams()
        self.deduplicate()
        self.extract_job_level()
        self.match_and_count()
        self.update_totals()
        #self.build_results()
        end = datetime.now()
        delta = end - start
        print(f"Process took: {str(delta)[:-3]}")
        with open("counts.json", "w") as f:
            json.dump(self._buckets, f, indent=4)

    def deduplicate(self):
        duplicates = set()
        processed = 0
        iterations = 0
        start = datetime.now()
        for index_a, listing_a in self._listings.items():
            processed += 1
            # cache reference for set_a outside the inner loop
            unigrams_a = self._indexed_ngrams[index_a].get("description").get("unigrams")
            for index_b, listing_b in self._listings.items():
                iterations += 1

                # skip if same index
                if index_a == index_b: continue

                # early detect if external_id available and same
                if listing_a.external_id and listing_b.external_id:
                    if listing_a.external_id == listing_b.external_id:
                        duplicates.add(index_b)
                        continue

                # early detect if description is exactly the same
                if listing_a.description == listing_b.description:
                    duplicates.add(index_b)
                    continue

                unigrams_b = self._indexed_ngrams[index_b].get("description").get("unigrams")

                # get the size of intersection A in B
                intersection_count = len(unigrams_a & unigrams_b)
                if not intersection_count: continue

                # get the size of union A with B
                union_count = len(unigrams_a) + len(unigrams_b) - intersection_count

                if not union_count: continue

                # calculate Jaccard index by dividing intersection per union
                jaccard_sim = intersection_count / union_count

                # if sim above 90% mark it as duplicate
                if jaccard_sim >= 0.90:
                    duplicates.add(index_b)
                    continue
                    #print(f"Near duplicate detected for {index_a} in {index_b}: {jaccard_sim}")

                # If between 80% and 90% we check title as tie-breaker
                if 0.80 <= jaccard_sim <= 0.90:
                    #print(f"some similarity detected for {index_a} in {index_b}: {jaccard_sim}")
                    #print(f"using title as tie-braker: {listing_a.title} == {listing_b.title}?")
                    if listing_a.title == listing_b.title:
                        #print("Considered duplicate")
                        duplicates.add(index_b)

        for index in duplicates:
            self._listings.pop(index)


        end = datetime.now()
        delta = end - start

    def match_and_count(self) -> None:
        # initialize buckets
        self.generate_buckets()

        # iterate over listings
        for index, _listing in self._listings.items():
            # get ngrams for current listing
            _ngrams: set[str] = self._indexed_ngrams[index]["ngrams"]

            if not _ngrams:
                continue

            # process against topics
            for topic in self.topics:
                matched_terms = TextProcessor.find_matches(_ngrams, topic.terms)

                # update deepest bucket
                if matched_terms:
                    mutated_job_level = self._indexed_job_level.get(_listing.id)
                    self.update_buckets(topic.title, mutated_job_level, matched_terms)
            self.listing_processed()

    def extract_ngrams(self) -> None:
        # Note: currently this is too manual for my taste, need to abstract
        if not self._listings:
            pass

        self._indexed_ngrams = {}
        _metrics = {"processed": 0}
        for index, _listing in self._listings.items():

            title = TextProcessor.remove_stopwords(_listing.title)
            title_unigrams: set[str] = set(TextProcessor.extract_unigrams(title))
            title_bigrams: set[str] = set(TextProcessor.extract_bigrams(title))
            title_ngrams: set[str] = title_unigrams | title_bigrams

            description = TextProcessor.remove_stopwords(_listing.description)
            description_unigrams: set[str] = set(TextProcessor.extract_unigrams(description))
            description_bigrams: set[str] = set(TextProcessor.extract_bigrams(description))
            description_ngrams: set[str] = description_unigrams | description_bigrams

            combined_ngrams: set[str] = title_ngrams | description_ngrams

            self._indexed_ngrams[index] = {
                "title": {
                    "unigrams": title_unigrams,
                    "bigrams": title_bigrams,
                    "ngrams": title_ngrams,
                },
                "description": {
                    "unigrams": description_unigrams,
                    "bigrams": description_bigrams,
                    "ngrams": description_ngrams
                },
                "ngrams": combined_ngrams,
            }
            _metrics["processed"] += 1
        return _metrics

    def extract_job_level(self) -> dict[str, Any]:
        mappings = MappingsLoader.get_mappings()
        job_levels = mappings["canonical"].get("job_level")

        self._indexed_job_level: dict[str, str] = {}

        _metrics = {"processed": 0, "mutations": 0}
        for _index, _listing in self._listings.items():
            self._indexed_job_level[_listing.id] = _listing.job_level

            if job_levels:
                title_tokens = _listing.title.split()

                # Iterate over levels; Use reversed to respect hierarchy
                found = False
                for canonical in reversed(list(job_levels.keys())):
                    variations = set(job_levels[canonical])
                    for variation in variations:
                        if variation in title_tokens:
                            self._indexed_job_level[_listing.id] = canonical
                            if config.mode.dev:
                                print(f'found match for '
                                      f'"{canonical}" -> "{variation}" '
                                      f'in {_listing.id}: "{" ".join(title_tokens)}"')
                            found = True
                            _metrics["mutations"] += 1
                            break
                    if found:
                        break
            _metrics["processed"] += 1
            if config.mode.dev:
                print(_metrics)
        return _metrics

    def build_listings(self) -> None:
        if not self._session:
            pass

        self._listings: dict[int, DynamicListing] = {}

        for index, listing in self._session.listings.items():
            self._listings[index] = DynamicListingFactory.create(index, listing.raw_data)

    def update_totals(self) -> None:
        global_matches = self._buckets["total"]["matches_counter"]
        for topic in self.topics:
            topic_bucket = self._buckets[topic.title]
            for level_data in topic_bucket["per_level"].values():
                topic_bucket["listings_counter"] += level_data["listings_counter"]
                topic_bucket["matches_counter"].update(level_data["matches_counter"])
            global_matches.update(topic_bucket["matches_counter"])

    def listing_processed(self) -> None:
        self._buckets["total"]["listings_counter"] += 1

    def build_results(self):
        # TODO: Implement :P
        pass

    def generate_buckets(self):

        # get available job level mappings
        mappings: dict = MappingsLoader.get_mappings()
        canonical_mappings: dict = mappings.get("canonical")
        job_levels: dict = canonical_mappings.get("job_level", {})

        # initialize buckets with total counter
        self._buckets = {
            "total": {
                "listings_counter": 0,
                "matches_counter": Counter()
            }
        }

        # dynamically generate buckets for each topic
        for topic in self.topics:
            self._buckets[topic.title] = {
                "listings_counter": 0,
                "matches_counter": Counter(),
                "per_level": {}
            }

            # dynamically generate buckets for each job_level
            for level in job_levels.keys():
                self._buckets[topic.title]["per_level"][level] = {
                    "listings_counter": 0,
                    "matches_counter": Counter()
                }

    def update_buckets(self, topic: str, job_level, matches: set[str]) -> None:
        self._buckets[topic]["per_level"][job_level]["listings_counter"] += 1
        self._buckets[topic]["per_level"][job_level]["matches_counter"].update(matches)

    def sanitize_listings(self):
        for index, listing in self._listings.items():
            sanitized_title = TextProcessor.sanitize(listing.title)
            listing.title = sanitized_title

            sanitized_description = TextProcessor.sanitize(listing.description)
            listing.description = sanitized_description

    def _debug_dump(
            self,
            ngrams: bool = False,
            session: bool = False,
            topics: bool = False,
            listings: bool = False,
            indexed_job_levels: bool = False,
            indexed_ngrams: bool = False,
            buckets: bool = False,
            include_raw: bool = False,
            full: bool = False,
        ) -> None:

        if not config.mode.dev:
            pass

        timestamp = datetime.now()
        _filename = f"debug_dump_{timestamp.strftime("%Y%m%d_%H%M%S")}.json"

        print(f"DEBUG: Dumping Processor state at {config.data_dir}")

        _dump: dict[str, Any] = {"dumped_at": timestamp.isoformat(), "metrics": self._metrics}

        # session update
        if session or full:
            _dump["session"] = {}

            if not self._session:
                _dump["session"] = "Not Assigned"

            else:
                _dump["session"] = {
                    "id": self._session.id,
                    "title": self._session.title,
                    "description": self._session.description,
                    "start_time": self._session.start_time.isoformat(),
                    "finish_time": self._session.finish_time.isoformat(),
                    "meta": self._session.meta,
                    "listings": len(self._session.raw_listings),
                }

        # dump topics loaded
        if topics or full:
            _dump["topics"] = {}

            if not self._topics:
                _dump["topics"] = "Not Assigned"
            else:
                for index, topic in enumerate(self.topics):
                    _dump["topics"][index] = {
                        "title": topic.title,
                        "description": topic.description,
                        "terms": topic.terms,
                    }

        # dump processed listings
        if listings or full:
            _dump["listings"] = {}

            if not self._listings:
                _dump["listings"] = "Not Assigned"

            else:
                for index, _listing in self._listings.items():
                    _dump["listings"][index]= {
                        "id": _listing.id,
                        "title": _listing.title,
                        "job_level": _listing.job_level,
                        "description": _listing.description,
                        "external_id": _listing.external_id,
                        "raw_data": json.loads(self._session.raw_listings[index]),
                    }

        # dump indexed ngrams
        if ngrams:
            for key, value in self.indexed_ngrams.items():
                _dump["indexed_ngrams"][key] = ({
                    "listing_id": key,
                    "words": {
                        "count": len(value),
                        "bags": {
                            "unigrams": list(value["unigrams"]),
                            "bigrams": list(value["bigrams"]),
                            "ngrams": list(value["ngrams"]),
                        }
                    }
                })

        # save to data path
        with open(config.data_dir.joinpath("debug.json"), "w") as out:
            json.dump(_dump, out, indent=2)
        with open(config.data_dir.joinpath("counts.json"), "w") as out:
            json.dump(self.buckets, out, indent=2)