from collections import Counter
import unicodedata
import html
import re

from pandas.core.computation.expr import intersection


class TextProcessor:
    UNICODE_REPLACEMENTS: dict[str, list[str]] = {
        # Dashes
        "-": [
            "&mdash;", "&#8212;", "&#x2014;", "\u2014",
            "&ndash;", "&#8211;", "&#x2013;", "\u2013",
            "&#8208;", "&#x2010;", "\u2010",
            "&#8209;", "&#x2011;", "\u2011",
            "&#8210;", "&#x2012;", "\u2012",
            "&#8213;", "&#x2015;", "\u2015"
        ],

        # Single Quotes / Apostrophes
        "'": [
            "&rsquo;", "&#8217;", "&#x2019;", "\u2019",
            "&lsquo;", "&#8216;", "&#x2018;", "\u2018",
            "&apos;", "&#39;", "&#x27;", "\u0027"
        ],

        # Double Quotes
        '"': [
            "&rdquo;", "&#8221;", "&#x201d;", "\u201d",
            "&ldquo;", "&#8220;", "&#x201c;", "\u201c",
            "&quot;", "&#34;", "&#x22;", "\u0022"
        ],

        # Non-breaking Spaces
        " ": ["&nbsp;", "&#160;", "&#xa0;", "\u00a0"],

        # Ellipsis
        "...": ["&hellip;", "&#8230;", "&#x2026;", "\u2026"]
    }
    STOPWORDS: set[str] =[
        "a", "o", "e", "de", "do", "da", "em", "um", "uma", "para", "com", "por",
        "se", "no", "na", "os", "as", "ao", "aos", "nas", "dos", "das", "que",
        "ou", "sua", "seu", "suas", "seus", "tem", "nosso", "nossa", "nossos",
        "nossas", "mais", "pelo", "pela", "como", "quem", "ser", "foi", "está",
        "estão", "é", "era", "são", "fomos", "foram", "têm", "tinha", "tinham",
        "eu", "tu", "ele", "ela", "nós", "vós", "eles", "elas", "me", "te",
        "lhe", "nos", "vos", "lhes", "meu", "minha", "teu", "tua", "este",
        "esta", "isto", "esse", "essa", "isso", "aquele", "aquela", "aquilo",
        "mas", "nem", "ou", "porque", "então", "logo", "pois", "muito", "também",
        "the", "and", "a", "to", "of", "in", "is", "you", "that", "it", "he",
        "was", "for", "on", "are", "as", "with", "his", "they", "i", "at",
        "be", "this", "have", "from", "or", "one", "had", "by", "but", "not",
        "what", "all", "were", "we", "when", "your", "can", "said", "there",
        "use", "an", "each", "which", "she", "do", "how", "their", "if", "will",
        "up", "other", "about", "out", "many", "then", "them", "these", "so",
        "some", "her", "would", "make", "like", "him", "into", "time", "has",
        "look", "two", "more", "write", "go", "see", "no", "way", "could",
        "people", "my", "than", "first", "been", "call", "who", "its", "now",
        "find", "did", "down", "come", "made", "may", "part"

    ]

    @classmethod
    def remove_stopwords(cls, text: str) -> str:
        return " ".join([
            word for word
            in text.split()
            if word not in cls.STOPWORDS
        ])

    @classmethod
    def sanitize(cls, text: str) -> str:
        if not text:
            return ""

        # Replace unicode codes with ascii variations
        for replacement, variations in cls.UNICODE_REPLACEMENTS.items():
            for variant in variations:
                text = text.replace(variant, replacement)

        # decode any other unicode code to whatever (will strip it down later)
        text = html.unescape(text)

        # lowercase everything
        text: str = text.lower()

        # remove accents marks and other non ASCII
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')

        # replace any of these with whitespace
        text = re.sub(r'[,*(|)!?:\\/&;]', " ", text)

        # replace dot wrapped by spaces
        text = re.sub(r'(?<!\w)\.|\.(?!\w)', " ", text)

        # replace if leaded by a whitespace
        text = re.sub(r'\s[#+](?=[a-zA-Z0-9])', " ", text)

        # replace any dot followed by a whitespace
        text = re.sub(r'(?<=\S)\.+(?=\s|$)', " ", text)

        # replace dashes if they are leaded by a whitespace
        text = re.sub(r'\s-(?=[a-zA-Z])', " ", text)

        # collapse n whitespaces into a single whitespace
        text = re.sub(r'\s+', " ", text)

        # strip whitespace from the edges
        text = text.strip()

        return text

    @staticmethod
    def extract_unigrams(text: str) -> list[str]:
        return text.split()

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