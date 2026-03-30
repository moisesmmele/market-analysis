from app.persistence import Database

class Deduplicator:
    database: Database = None
    duplicate_pairs: set[tuple[int, int]] = None
    duplicate_indices: set[int] = None
    processed: int = None
    iterations: int = None

    def __init__(self):
        self.database = Database()

    @staticmethod
    def calculate_jaccard(set_a: set[str], set_b: set[str]) -> float:
        intersection_count = len(set_a & set_b)
        if not intersection_count: return 0.0
        union_count = len(set_a) + len(set_b) - intersection_count
        if not union_count: return 0.0
        return intersection_count / union_count

    def run(self, session_id: int, sets: dict[int, set[str]]) -> set[int]:
        self.iterations = 0
        self.processed = 0

        indexed_sets = sets.copy()

        known_duplicates = self.load_duplicate_pairs(session_id)

        indices = list(indexed_sets.keys())
        for index in indices.copy():
            if index in known_duplicates:
                indexed_sets.pop(index)
                indices.remove(index)

        for i, index_a in enumerate(indices):
            set_a = indexed_sets[index_a]
            print(i, index_a)
            for index_b in indices[i+1:]:
                print(index_b)
                set_b = indexed_sets[index_b]
                pair = (index_a, index_b)

                jaccard_sim = self.calculate_jaccard(set_a, set_b)

                if jaccard_sim >= 0.90:
                    self.duplicate_pairs.add(pair)
                    continue

                self.iterations += 1
            self.processed += 1

        self.save_duplicate_pairs(session_id)
        return self.get_duplicate_indices()

    def get_duplicate_indices(self) -> set[int]:
        duplicates = set()
        for pair in self.duplicate_pairs:
            duplicates.add(pair[0])
            duplicates.add(pair[1])
        return duplicates

    def load_duplicate_pairs(self, session_id: int) -> set[int]:
        self.duplicate_pairs = set(self.database.get_duplicates(session_id))
        return self.get_duplicate_indices()

    def save_duplicate_pairs(self, session_id: int):
        new_duplicates =  list(self.duplicate_pairs ^ set(self.database.get_duplicates(session_id)))
        if new_duplicates:
            self.database.save_duplicates(session_id, new_duplicates)