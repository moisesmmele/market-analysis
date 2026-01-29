from listing_factory import ListingFactory
from mappings_loader import MappingsLoader
from text_processor import TextProcessor
from topic_loader import TopicLoader
from collections import Counter
from database import Database
from datetime import datetime
from listing import Listing
from session import Session
from config import config
from topic import Topic
from typing import Any
import json

class Processor:

    indexed_ngrams: dict[str, dict[str, set[str]]] = None
    indexed_job_level: dict[str, list] = None
    buckets: dict[str, dict[str, Any]] = None
    results: list[dict[str, Any]] = None
    listings: list[Listing] = None
    topics: list[Topic] = None
    session: Session = None

    # TODO: 1. IMPLEMENT BOOL OR METRICS RETURNS FOR EVERY METHOD THAT RETURNS NONE / HAS EFFECT / CHANGES STATE
    # TODO: 2. IMPLEMENT GUARD CLAUSES / EARLY RETURN ON PROCESS METHOD

    def __init__(self, session: Session, topics: list[Topic]) -> None:
        self.session = session
        self.topics = topics

    def process(self) -> Any:
        self.build_listings()
        self.extract_ngrams()
        self.extract_job_level()
        self.match_and_count()
        self.update_totals()
        self._debug_dump()
        self.build_results()

    def match_and_count(self) -> None:
        # initialize buckets
        self.generate_buckets()

        # iterate over listings
        for _listing in self.listings:
            # get ngrams for current listing
            _ngrams: set[str] = self.indexed_ngrams.get(_listing.id).get("ngrams")

            if not _ngrams:
                continue

            # process against topics
            for topic in self.topics:
                matched_terms = TextProcessor.find_matches(_ngrams, topic.terms)

                # update deepest bucket
                if matched_terms:
                    mutated_job_level = self.indexed_job_level.get(_listing.id)
                    self.update_buckets(topic.title, mutated_job_level, matched_terms)
            self.listing_processed()

    def extract_ngrams(self) -> None:
        # Initialize index
        self.indexed_ngrams: dict[str, dict[str, set[str]]] = {}

        # iterate over listings
        for _listing in self.listings:
            # sanitize original values
            description = TextProcessor.sanitize(_listing.description)
            _title = TextProcessor.sanitize(_listing.title)

            # combine it for more certainty
            combined = f"{description} {_title}"

            # get unigrams
            _unigrams = set(combined.split())

            #extract bigrams
            _bigrams = set(TextProcessor.extract_bigrams(combined))

            indexed_ngrams = {
                "unigrams": _unigrams,
                "bigrams": _bigrams,
                "ngrams": _unigrams | _bigrams,
            }

            self.indexed_ngrams[_listing.id] = indexed_ngrams

    def extract_job_level(self) -> None:
        print(f"DEBUG: Hit Processor.extract_job_level()")
        # load mappings from cached
        mappings = MappingsLoader.get_mappings()
        # get canonical map for job_level field
        job_levels = mappings["canonical"].get("job_level")
        # zero/initialize index
        self.indexed_job_level: dict[str, str] = {}

        # iterate over loaded listings
        for _listing in self.listings:
            #print(f"DEBUG: iterating over self.listings - currently on {_listing.id}")
            # defaults to current value
            self.indexed_job_level[_listing.id] = _listing.job_level
            #print(f"DEBUG: Defaulted to current state: {_listing.job_level}")

            # if we have custom mappings for job_level
            if job_levels:
                # tokenize title
                tokens = TextProcessor.sanitize(_listing.title).split()
                #print(f"DEBUG: Sanitized title tokens: {tokens}")
                # iterate over possible levels, using reversed to respect hierarchy over experience
                found = False
                for canonical in reversed(list(job_levels.keys())):
                    #print(f"DEBUG: Iterating over possible job-levels for: {canonical}")
                    variations = set(job_levels[canonical])
                    #print(f"DEBUG: Current possible variations: {variations}")
                    # iterate over possible variations
                    for variation in variations:
                        # if we match a variation with any title token
                        #print(f"DEBUG: Checking if {variation} is in {tokens}")
                        if variation in tokens:
                            self.indexed_job_level[_listing.id] = canonical
                            print(f"DEBUG: Found: {variation} in {_listing.title}")
                            print(f"DEBUG: Current: {_listing.job_level} Indexed: {canonical}")
                            # signal to break outer loop
                            found = True
                            # break inner loop
                            break
                    if found:
                        break

    def build_listings(self) -> None:
            self.listings: list[Listing] = []
            for _listing in self.session.raw_listings:
                self.listings.append(ListingFactory.create(_listing['id'],_listing['raw_data']))

    def update_totals(self) -> None:
        global_matches = self.buckets["total"]["matches_counter"]
        for topic in self.topics:
            topic_bucket = self.buckets[topic.title]
            for level_data in topic_bucket["per_level"].values():
                topic_bucket["listings_counter"] += level_data["listings_counter"]
                topic_bucket["matches_counter"].update(level_data["matches_counter"])
            global_matches.update(topic_bucket["matches_counter"])

    def listing_processed(self) -> None:
        self.buckets["total"]["listings_counter"] += 1

    def build_results(self):
        # TODO: Implement :P
        pass

    def generate_buckets(self):

        # get available job level mappings
        mappings: dict = MappingsLoader.get_mappings()
        canonical_mappings: dict = mappings.get("canonical")
        job_levels: dict = canonical_mappings.get("job_level", {})

        # initialize buckets with total counter
        self.buckets = {
            "total": {
                "listings_counter": 0,
                "matches_counter": Counter()
            }
        }

        # dynamically generate buckets for each topic
        for topic in self.topics:
            self.buckets[topic.title] = {
                "listings_counter": 0,
                "matches_counter": Counter(),
                "per_level": {}
            }

            # dynamically generate buckets for each job_level
            for level in job_levels.keys():
                self.buckets[topic.title]["per_level"][level] = {
                    "listings_counter": 0,
                    "matches_counter": Counter()
                }

    def update_buckets(self, topic: str, job_level, matches: set[str]) -> None:
        self.buckets[topic]["per_level"][job_level]["listings_counter"] += 1
        self.buckets[topic]["per_level"][job_level]["matches_counter"].update(matches)

    def _debug_dump(self, ngrams: bool = False) -> None:
        timestamp = datetime.now().isoformat()
        _dump = {
            "dumped_at": timestamp,
            "session": {},
            "topics": {},
            "listings": {},
            "indexed_ngrams": {},
            "indexed_job_level": self.indexed_job_level,
            "buckets": self.buckets,
        }
        # session update
        _dump["session"].update({
            "id": self.session.id,
            "title": self.session.title,
            "description": self.session.description,
            "start_time": self.session.start_time.isoformat(),
            "finish_time": self.session.finish_time.isoformat(),
            "meta": self.session.meta,
            "listings": len(self.session.raw_listings),
        })

        # dump topics loaded
        for index, topic in enumerate(self.topics):
            _dump["topics"][index] = ({
                "title": topic.title,
                "description": topic.description,
                "terms": topic.terms,
            })

        # dump processed listings
        raw_listing_map = {item['id']: item for item in self.session.raw_listings}
        for index, _listing in enumerate(self.listings):
            matching_raw_list = raw_listing_map.get(_listing.id)
            parsed_matching_raw_list = json.loads(matching_raw_list["raw_data"])
            _dump["listings"][index]=({
                "id": _listing.id,
                "title": _listing.title,
                "job_level": _listing.job_level,
                "description": _listing.description,
                "external_id": _listing.external_id,
                "sanitized": {
                    "title": TextProcessor.sanitize(parsed_matching_raw_list["title"]),
                    "description": TextProcessor.sanitize(parsed_matching_raw_list["description"]),
                },
                "raw_data": parsed_matching_raw_list,
            })

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

