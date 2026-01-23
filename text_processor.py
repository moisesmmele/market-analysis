from collections import Counter
import unicodedata
import re

class TextProcessor:
    def __init__(self):
        pass

    def sanitize(self, text: str) -> str:
        if not isinstance(text, str):
            return ""
    
        # 1. Lowercase
        text = text.lower()
    
        # 2. Pre-process specific tech terms
        replace_pattern = re.compile(r'[^a-z0-9\s]')
        replacements = {
            "c#": "csharp",
            "c++": "cpp",
            ".net": "dotnet",
            "node.js": "nodejs",
            "react.js": "react"
        }
        for term, replacement in replacements.items():
            if term in text: # simple check to avoid unnecessary replace calls
                text = text.replace(term, replacement)

        # 3. Normalize unicode
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    
        # 4. Remove special chars
        text = re.sub(replace_pattern, ' ', text)
    
        return text

    def extract_bigrams(self, text: str) -> list[str]:
        words: list[str] = text.split()
        if len(words) < 2:
            return []

        bigrams: list[str] = [f"{words[i]} {words[i + 1]}" for i in range(len(words) - 1)]

        return bigrams

    def count_bigrams(self, text: str, bigrams: set[str]) -> dict[str, int]:
        all_bigrams: list[str] = self.extract_bigrams(text)
        counted: Counter[str] = Counter(all_bigrams)
        
        result: dict[str, int] = {
            k: counted[k] 
            for k in bigrams 
            if k in counted
        }

        return result

    def count_words(self, text: str) -> dict[str, int]:
        sanitized: str = self.sanitize(text)
        words: list[str] = sanitized.split()
        counted: Counter[str] = Counter(words)
        return dict(counted)

    def count_keywords(self, text: str, keywords: set[str]) -> dict[str, int]:
        words: list[str] = text.split()
        counted: Counter[str] = Counter(words)
        
        result: dict[str, int] = {
            k: counted[k] 
            for k in keywords 
            if k in counted
        }
        
        return result

    def remove_stopwords(self, text: str, stopwords: set[str]) -> str:
        sanitized: str = self.sanitize(text)
        words: list[str] = sanitized.split()
        filtered: list[str] = [w for w in words if w not in stopwords]
        return ' '.join(filtered)