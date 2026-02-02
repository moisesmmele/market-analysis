import unicodedata
import ftfy
import html
import re

class TextProcessor:

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
    VIP_CHARS = {'+', '#', '.', ',', '-', '$', ' ', "'"}

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

        # fix broken encoding
        text = ftfy.fix_text(text)

        # convert HTML Entities into Unicode
        text = html.unescape(text)

        # Remove weird/uncommon characters
        # Normalize Unicode Compatibility Characters to Canonical Characters
        # (Normalization Form - (K)Compatibility Composition)
        text = unicodedata.normalize('NFKC', text)

        # strip accent marks - stage 1
        # Decompose Unicode Precomposed Characters to Canonical Decomposed Sequences Equivalents
        # (Normalization Form - (Canonical) Decomposition)
        text = unicodedata.normalize('NFD', text)

        # Individual char replacements based on Unicode Category
        # Iterate over Text and assert its category
        chars = []
        for index, character in enumerate(text):
            # UC means Unicode Category
            uc = unicodedata.category(character)

            # Allow Any letters
            if uc.startswith('L'):
                chars.append(character)
                continue

            # Allow Numbers
            if uc.startswith('N'):
                chars.append(character)
                continue

            # Strip Accent Marks - Stage 2
            # If Unicode Category is "Mark, nonspacing", ignore it
            if uc == "Mn":
                continue

            # Remove emojis and other symbols
            # If UC is "Symbol, other", ignore it
            if uc == "So":
                continue

            # Remove control chars
            # If UC is part of "Control" upper category, ignore it
            if uc.startswith('C'):
                continue

            # normalize spaces
            # If UC is part of "Separator" upper category,
            # replace it with a standard space
            if uc.startswith('Z'):
                chars.append(' ')
                continue

            # Normalize Currency Marks
            # If UC is "Symbol, currency",
            # Replace it with a dollar sign
            if uc == 'Sc':
                chars.append("$")
                continue

            # Normalize Dashes
            # If UC is "Punctuation, dash",
            # Replace it with a hyphen
            if uc == 'Pd':
                if index < 1 or index > len(text) - 1:
                    leading = text[index - 1]
                    leading_uc = unicodedata.category(leading)
                    trailing = text[index + 1]
                    trailing_uc = unicodedata.category(trailing)
                    if leading_uc.startswith('L') or trailing_uc.startswith('L'):
                        chars.append("-")
                    else:
                        chars.append(" ")
                    continue

            # Normalize Quotes
            # If UC is "Punctuation, initial" or "Punctation, final"
            # Replace it with a standard quotation mark
            if uc in ("Pi", "Pf"):
                chars.append('"')
                continue

            # If no other matches, check VIP list
            if character in cls.VIP_CHARS:
                chars.append(character)
                continue

            # if still no matches, replace it with a whitespace
            chars.append(" ")

        text = "".join(chars)

        # Lowercase everything
        text: str = text.lower()
        #print(f"text before regex:\n{text}")

        # Remove ill escapes
        text = re.sub(r'\\', "", text)

        # Remove tech-safe punctuation marks
        text = re.sub(r'[*(|)!?:/;,]', " ", text)

        # Remove other standalone punctuation that is not between word characters
        text = re.sub(r'(?<!\w)[.#+-](?!\w)', " ", text)

        # Remove common, tech-safe leading symbols for word characters
        text = re.sub(r'\s[.#+-](?=[a-zA-Z0-9])', " ", text)

        # Remove trailing punctuation leaded by any char and trailed by white space
        text = re.sub(r'(?<=\w)[.,](?=\s|$)', " ", text)

        # Collapse multiple whitespaces into single space
        text = re.sub(r'\s+', " ", text)

        # Strip whitespace from edges
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