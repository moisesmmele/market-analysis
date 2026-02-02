from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any
import json

from app.entities import DynamicListing, Session, Topic, Metric
from app.components import DynamicListingFactory
from app.components import TextProcessor
from app.loaders import MappingsLoader
from app import config
from enums import InfoType


class Processor:

    _ngrams = None
    _job_levels: dict[str, list] = None
    _buckets: dict[str, dict[str, Any]] = None
    _listings: dict[int, DynamicListing] = None
    _metrics: dict[str, Metric] = None
    _topics: list[Topic] = None
    _session: Session = None

    results: list[dict[str, Any]] = None

    # TODO: 1. IMPLEMENT BOOL OR METRICS RETURNS FOR EVERY METHOD THAT RETURNS NONE / HAS EFFECT / CHANGES STATE
    # TODO: 2. IMPLEMENT GUARD CLAUSES / EARLY RETURN ON PROCESS METHOD

    def __init__(self, session: Session, topics: list[Topic]) -> None:
        self._session = session
        self._topics = topics
        self._metrics = dict()

    def process(self) -> Metric:
        metric = Metric("process")

        self.build_listings()
        self.sanitize_listings()
        self.extract_ngrams()
        self.deduplicate()
        self.extract_job_level()
        self.match_and_count()
        self.update_totals()
        #self.build_results()

        metric.success()
        metric.append_info("total_steps", 7)
        return self.append_metric(metric)

    def deduplicate(self) -> Metric:
        metric = Metric("deduplicate")

        duplicates = set()
        processed = 0
        iterations = 0

        for index_a, listing_a in self._listings.items():
            processed += 1
            # cache reference for set_a outside the inner loop
            unigrams_a = self._ngrams[index_a].get("description").get("unigrams")
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

                unigrams_b = self._ngrams[index_b].get("description").get("unigrams")

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

        metric.success()
        metric.append_info("duplicates", len(duplicates))
        metric.append_info("iterations", iterations)
        metric.append_info("processed", processed)
        return self.append_metric(metric)

    def match_and_count(self) -> Metric:
        metric = Metric("match_and_count")

        self.generate_buckets()

        iterations = 0
        processed = 0

        if not self._ngrams:
            metric.failure()
            metric.append_info("failure", "No ngrams available")
            return self.append_metric(metric)

        if not self._topics:
            metric.failure()
            metric.append_info("failure", "No topics available")
            return self.append_metric(metric)

        for index, _listing in self._listings.items():
            # get ngrams for current listing
            _ngrams: set[str] = self._ngrams[index]["ngrams"]
            if not _ngrams:
                message = {"message": f"No ngrams available for listing {index}"}
                metric.append_info("info", message, InfoType.WARNING)
                continue
            matches = 0
            # process against topics
            for topic in self._topics:
                matched_terms = TextProcessor.find_matches(_ngrams, topic.terms)
                iterations += 1
                if matched_terms:
                    matches += 1
                # update deepest bucket
                mutated_job_level = self._job_levels.get(_listing.id)
                self.update_buckets(topic.title, mutated_job_level, matched_terms)

            if matches == 0:
                message = f"No matches for listing {_listing.id}"
                metric.append_info("message", message, InfoType.WARNING)
                continue

            processed += 1
            self.listing_processed()

        metric.success()
        metric.append_info("processed", processed)
        metric.append_info("iterations", iterations)
        return self.append_metric(metric)

    def extract_ngrams(self) -> Metric:
        # Note: currently this is too manual for my taste, need to abstract
        metric = Metric("extract_ngrams")

        if not self._listings:
            metric.failure()
            metric.append_info("failure", f"No listings to extract ngrams for")
            return self.append_metric(metric)

        self._ngrams = {}
        processed: int = 0
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

            self._ngrams[index] = {
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

            processed += 1

        metric.success()
        metric.append_info("processed", processed)
        return self.append_metric(metric)

    def extract_job_level(self) -> Metric:
        metric = Metric("extract_job_level")

        mappings = MappingsLoader.get_mappings()
        if not mappings:
            metric.failure()
            metric.append_info("failure", "No mappings available")
            return self.append_metric(metric)

        job_levels = mappings["canonical"].get("job_level")
        if not job_levels:
            metric.failure()
            metric.append_info("failure", "No job levels available")
            return self.append_metric(metric)

        if not self._listings:
            metric.failure()
            metric.append_info("failure", "No listings available")
            return self.append_metric(metric)

        self._job_levels: dict[str, str] = {}

        processed: int = 0
        iterations: int = 0
        mutations: int = 0
        for _index, _listing in self._listings.items():
            # default to current value
            self._job_levels[_listing.id] = _listing.job_level

            title_tokens = _listing.title.split()

            # Iterate over levels; Use reversed to respect hierarchy
            found = False
            for canonical in reversed(list(job_levels.keys())):
                variations = set(job_levels[canonical])
                for variation in variations:
                    iterations += 1
                    if variation in title_tokens:
                        self._job_levels[_listing.id] = canonical
                        found = True
                        mutations += 1
                        break
                if found:
                    break
            processed += 1

        metric.success()
        metric.append_info("processed", processed)
        metric.append_info("mutations", mutations)
        metric.append_info("iterations", iterations)
        return self.append_metric(metric)

    def build_listings(self) -> Metric:
        metric = Metric("build_listings")
        if not self._session:
            metric.failure()
            metric.append_info("failure", "No session available")
            return self.append_metric(metric)

        self._listings: dict[int, DynamicListing] = {}

        processed: int = 0
        for index, listing in self._session.listings.items():
            self._listings[index] = DynamicListingFactory.create(index, listing.raw_data)
            processed += 1

        metric.success()
        metric.append_info("processed", processed)
        metric.append_info("created", len(self._listings))
        return self.append_metric(metric)

    def update_totals(self) -> Metric:
        metric = Metric("update_totals")
        if not self._buckets:
            metric.failure()
            metric.append_info("failure", "No buckets available")
            return self.append_metric(metric)

        processed: int = 0
        global_matches = self._buckets["total"]["matches_counter"]
        for topic in self._topics:
            topic_bucket = self._buckets[topic.title]
            for level_data in topic_bucket["per_level"].values():
                topic_bucket["listings_counter"] += level_data["listings_counter"]
                topic_bucket["matches_counter"].update(level_data["matches_counter"])
                processed += 1
            global_matches.update(topic_bucket["matches_counter"])
        metric.success()
        return self.append_metric(metric)

    def listing_processed(self) -> None:
        self._buckets["total"]["listings_counter"] += 1

    def build_results(self):
        # TODO: Implement :P
        pass

    def generate_buckets(self):
        metric = Metric("generate_buckets")
        # get available job level mappings
        mappings: dict = MappingsLoader.get_mappings()
        if not mappings:
            metric.failure()
            metric.append_info("failure", "No mappings available")
            return self.append_metric(metric)

        canonical_mappings: dict = mappings.get("canonical")
        if not canonical_mappings:
            metric.failure()
            metric.append_info("failure", "No canonical mappings available")
            return self.append_metric(metric)

        job_levels: dict = canonical_mappings.get("job_level", {})
        if not job_levels:
            metric.failure()
            metric.append_info("failure", "No job levels available")
            return self.append_metric(metric)

        # initialize buckets with total counter
        self._buckets = {
            "total": {
                "listings_counter": 0,
                "matches_counter": Counter()
            }
        }

        processed: int = 0
        # dynamically generate buckets for each topic
        for topic in self._topics:
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
                processed += 1

        metric.success()
        metric.append_info("processed", processed)
        return self.append_metric(metric)

    def update_buckets(self, topic: str, job_level, matches: set[str]) -> None:
        self._buckets[topic]["per_level"][job_level]["listings_counter"] += 1
        self._buckets[topic]["per_level"][job_level]["matches_counter"].update(matches)

    def sanitize_listings(self):
        metric = Metric("sanitize_listings")
        processed = 0
        for index, listing in self._listings.items():
            sanitized_title = TextProcessor.sanitize(listing.title)
            listing.title = sanitized_title

            sanitized_description = TextProcessor.sanitize(listing.description)
            listing.description = sanitized_description
            processed+=1

        metric.success()
        metric.append_info("processed", processed)
        return self.append_metric(metric)

    def append_metric(self, metric: Metric) -> Metric:
        context = metric.get_context()
        self._metrics[context] = metric
        return metric

    def _debug_dump(
            self,
            session: bool = False,
            topics: bool = False,
            listings: bool = False,
            job_levels: bool = False,
            ngrams: bool = False,
            buckets: bool = False,
            include_raw: bool = False,
            full: bool = False,
        ) -> None:

        if not config.mode.dev:
            pass

        print("DEBUG: Starting debug dump...")
        debug_dir: Path = config.dir.debug

        timestamp = datetime.now()
        metrics = {index: metric.to_dict() for index, metric in self._metrics.items()}
        dump: dict[str, Any] = {"dumped_at": timestamp.isoformat(), "metrics": metrics}
        _master_filename = f"{timestamp.strftime("%Y%m%d_%H%M%S")}_processor_debug_dump.json"
        with open(debug_dir.joinpath(_master_filename), "w") as out:
            json.dump(dump, out, indent=2)

        # session update
        if session or full:
            dump_type = "session"
            dump: dict[str, Any] = {"dumped_at": timestamp.isoformat(), "dump_type": dump_type}
            _filename = f"{timestamp.strftime("%Y%m%d_%H%M%S")}_processor_{dump_type}.json"

            if not self._session:
                dump["session"] = "Not Assigned"

            else:
                dump["session"] = {
                    "id": self._session.id,
                    "title": self._session.title,
                    "description": self._session.description,
                    "start_time": self._session.start_time.isoformat(),
                    "finish_time": self._session.finish_time.isoformat(),
                    "meta": self._session.meta,
                    "listings": len(self._session.listings),
                }

            with open(debug_dir.joinpath(_filename), "w") as out:
                json.dump(dump, out, indent=2)

        # dump topics loaded
        if topics or full:
            dump_type = "topics"
            dump: dict[str, Any] = {"dumped_at": timestamp.isoformat(), "dump_type": dump_type}
            _filename = f"{timestamp.strftime("%Y%m%d_%H%M%S")}_processor_{dump_type}.json"

            if not self._topics:
                dump["topics"] = "Not Assigned"
            else:
                dump["topics"] = {}
                for index, topic in enumerate(self._topics):
                    dump["topics"][index] = {
                        "title": topic.title,
                        "description": topic.description,
                        "terms": topic.terms,
                    }

            with open(debug_dir.joinpath(_filename), "w") as out:
                json.dump(dump, out, indent=2)

        # dump processed listings
        if listings or full:
            dump_type = "listings"
            dump: dict[str, Any] = {"dumped_at": timestamp.isoformat(), "dump_type": dump_type}
            _filename = f"{timestamp.strftime("%Y%m%d_%H%M%S")}_processor_{dump_type}.json"

            if not self._listings:
                dump["listings"] = "Not Assigned"

            else:
                dump["listings"] = {}
                for index, _listing in self._listings.items():
                    dump["listings"][index]= {
                        "id": _listing.id,
                        "title": _listing.title,
                        "job_level": _listing.job_level,
                        "description": _listing.description,
                        "external_id": _listing.external_id,
                    }

            with open(debug_dir.joinpath(_filename), "w") as out:
                json.dump(dump, out, indent=2)

        # dump indexed ngrams
        if ngrams or full:
            dump_type = "ngrams"
            dump: dict[str, Any] = {"dumped_at": timestamp.isoformat(), "dump_type": dump_type}
            _filename = f"{timestamp.strftime("%Y%m%d_%H%M%S")}_processor_{dump_type}.json"

            if not self._ngrams:
                dump["ngrams"] = "Not Assigned"
            else:
                dump["ngrams"] = {}
                for index, bags in self._ngrams.items():
                    dump["ngrams"][index] = {
                        "listing_id": index,
                        "ngrams": list(bags["ngrams"]),
                        "title": {
                            "unigrams": list(bags["title"]["unigrams"]),
                            "bigrams": list(bags["title"]["bigrams"]),
                            "ngrams": list(bags["title"]["ngrams"]),
                        },
                        "description": {
                            "unigrams": list(bags["description"]["unigrams"]),
                            "bigrams": list(bags["description"]["bigrams"]),
                            "ngrams": list(bags["description"]["ngrams"]),
                        }
                    }

            with open(debug_dir.joinpath(_filename), "w") as out:
                json.dump(dump, out, indent=2)

        if job_levels or full:
            dump_type = "job_levels"
            dump: dict[str, Any] = {"dumped_at": timestamp.isoformat(), "dump_type": dump_type}
            _filename = f"{timestamp.strftime("%Y%m%d_%H%M%S")}_processor_{dump_type}.json"

            if not self._job_levels:
                dump["job_levels"] = "Not Assigned"
            else:
                dump["job_levels"] = {}
                for index, listing in self._listings.items():
                    dump["job_levels"][index] = {
                        "original": listing.job_level,
                        "inferred": self._job_levels[str(index)]
                    }

            with open(debug_dir.joinpath(_filename), "w") as out:
                json.dump(dump, out, indent=2)

        if buckets or full:
            dump_type = "buckets"
            dump: dict[str, Any] = {"dumped_at": timestamp.isoformat(), "dump_type": dump_type}
            _filename = f"{timestamp.strftime("%Y%m%d_%H%M%S")}_processor_{dump_type}.json"

            if not self._buckets:
                dump["buckets"] = "Not Assigned"
            else:
                dump["buckets"] = self._buckets

            with open(debug_dir.joinpath(_filename), "w") as out:
                json.dump(dump, out, indent=2)

        if include_raw or full:
            dump_type = "raw_listings"
            dump: dict[str, Any] = {"dumped_at": timestamp.isoformat(), "dump_type": dump_type}
            _filename = f"{timestamp.strftime("%Y%m%d_%H%M%S")}_processor_{dump_type}.json"

            if not self._session:
                dump["raw_listings"] = "Not Assigned"
            else:
                dump["raw_listings"] = {}
                for index, _listing in self._session.listings.items():
                    dump["raw_listings"][index]= {
                        "id": _listing.id,
                        "session_id": _listing.session_id,
                        "json": json.loads(_listing.raw_data)
                    }

            with open(debug_dir.joinpath(_filename), "w") as out:
                json.dump(dump, out, indent=2)

        print(f"DEBUG: Finished Debug Dump. Dumped Processor state at {debug_dir}\\.")
