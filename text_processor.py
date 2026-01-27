from collections import Counter
import unicodedata
import re

class TextProcessor:

    @staticmethod
    def sanitize(text: str) -> str:
        replacements: dict[str, str] = {
            "c#": "csharp",
            "c++": "cpp",
            ".net": "dotnet",
            "node.js": "nodejs",
            "react.js": "react"
        }

        text: str = text.lower()
        for term, replacement in replacements.items():
            if term in text:
                text = text.replace(term, replacement)
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        return text

    @staticmethod
    def extract_bigrams(text: str) -> list[str]:
        words: list[str] = text.split()
        bigrams: list[str] = [f"{words[i]} {words[i + 1]}" for i in range(len(words) - 1)]
        return bigrams

    @staticmethod
    def count_key_bigrams(bigrams: set[str], key_bigrams: set[str]) -> dict[str, int]:
        counted: Counter[str] = Counter(bigrams)
        result: dict[str, int] = {
            k: counted[k] 
            for k in bigrams 
            if k in key_bigrams
        }
        return result

    @staticmethod
    def count_words(text: str) -> dict[str, int]:
        words: list[str] = text.split()
        counted: Counter[str] = Counter(words)
        return dict(counted)

    @staticmethod
    def find_matches(source: set[str], words_to_match: dict[set[str]]) -> set[str]:
        matches: set[str] = set()
        for canonical, aliases in words_to_match.items():
            if canonical in source:
                matches.add(canonical)
                continue
            if set(aliases).intersection(source):
                matches.add(canonical)
        return matches

    @staticmethod
    def remove_stopwords(text: str, stopwords: set[str]) -> str:
        words: list[str] = text.split()
        filtered: list[str] = [w for w in words if w not in stopwords]
        return ' '.join(filtered)