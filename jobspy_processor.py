from collections import Counter

from database import Database
from jobspy_normalizer import JobspyNormalizer
from text_processor import TextProcessor
from session import Session
from config import config
from typing import Any
import pandas as pd
import json

class JobspyProcessor:
    session: Session = None
    session_df: pd.DataFrame = None
    keywords: list[dict[str, set]] = []
    counted_keywords: dict[str, dict[str, int]] = {}
    results: list[Any] = []

    def __init__(self, session: Session) -> None:
        self.session = session

    def process(self) -> list[Any]:
        self.load_keywords()
        self.extract_df()
        self.count_keywords(self.description_to_text())
        self.results.append(self.counted_keywords)
        self.results.append(self.session_df)
        return self.results


    def load_keywords(self):
        for file in config.keywords.glob("*.json"):
            with open(file, "r", encoding='utf-8') as f:
                self.keywords.append(json.load(f))

    def extract_df(self) -> None:
        self.session_df: pd.DataFrame = JobspyNormalizer.to_df(self.session.listings)

    def description_to_text(self) -> str:
        descriptions = self.session_df["description"]
        text: str = ""
        for description in descriptions:
            text += description + "\n"
        return text

    def count_keywords(self, text: str) -> None:
        for keyword_set in self.keywords:
            keys = keyword_set.keys()
            for name in keys:
                keywords = set(keyword_set[name])
                sanitized = TextProcessor.sanitize(text)
                counted = TextProcessor.count_keywords(sanitized, keywords)
                self.counted_keywords[name] = counted