from collections import Counter
import unicodedata
import html
import re

class TextProcessor:

    UNICODE_REPLACEMENTS: dict[str, list[str]] = {
            " - ":    ["&#8212;", "&mdash;", "\u2014", "&#8211;", "&ndash;", "\u2013", "\u2010", "\u2011", "\u2012", "\u2015"],
            "'":    ["&rsquo;", "&#8217;", "\u2019", "&lsquo;", "&#8216;", "\u2018", "&apos;", "&#39;"],
            '"':    ["&rdquo;", "&#8221;", "\u201d", "&ldquo;", "&#8220;", "\u201c", "&quot;", "&#34;"],
            " ":    ["&nbsp;", "&#160;", "\u00a0"],
            "...":  ["&hellip;", "&#8230;", "\u2026"],
    }

    @classmethod
    def sanitize(cls, text: str) -> str:
        if not text:
            return ""

        # lowercase everything
        text: str = text.lower()

        # Replace important unicode chars
        for replacement, variations in cls.UNICODE_REPLACEMENTS.items():
            for variant in variations:
                text = text.replace(variant, replacement)

        # decode html chars
        text = html.unescape(text)

        #text = text.replace(r'\"', '"')
        #text = text.replace(r"\'", "'")

        # forward slash
        text = re.sub(r'[/]', ' ', text)

        text = re.sub(r'[,*()\\\/]', '', text)

        # remove dot if preceded by non-whitespace and followed by whitespace (phrase endings)
        text = re.sub(r'(?<=\S)\.+(?=\s|$)', '', text)

        # collapse line skips, strip before and after spaces
        text = re.sub(r'\s+', ' ', text).strip()

        # remove accents marks
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')

        return text

    @staticmethod
    def extract_bigrams(text: str) -> list[str]:
        words: list[str] = text.split()
        bigrams: list[str] = [f"{words[i]} {words[i + 1]}" for i in range(len(words) - 1)]
        return bigrams

    @staticmethod
    def find_matches(source: set[str], terms_to_match: dict[str, list[str]]) -> set[str]:
        matches: set[str] = set()
        for canonical, aliases in terms_to_match.items():
            if canonical in source:
                matches.add(canonical)
                continue
            if set(aliases).intersection(source):
                matches.add(canonical)
        return matches