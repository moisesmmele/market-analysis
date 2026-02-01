from topic_loader import TopicLoader
from processor import Processor
from datetime import datetime
from database import Database
from session import Session
from random import random

def get_random_listing(database: Database, multiplier):
    random_id = int(random() * multiplier)
    listing = database.get_one_listing(random_id)
    if not listing:
        return get_random_listing(database, multiplier)
    return listing


topics = TopicLoader.get_available()
#selected_topics = []
#selected_topics.append(int(random() * len(topics)))
selected_topics = [i for i in topics.keys()]
random_topic = TopicLoader.select(selected_topics)

db = Database()
session = Session()
session.id = int(random() * 100)
session.title = "Random Sample Test"
session.description = "Lorem Ipsum dolor sit amet"
session.start_time = datetime(1970, 1, 1, 22, 59, 59)
session.finish_time = datetime(1970, 1, 1, 23, 59, 59)
session.meta = {"lorem": "ipsum", "foo": "bar"}

max_listings = db._query("SELECT COUNT(*) as count FROM listings")[0]["count"]

max_listing_samples = 20
session.listings = {
    listing.id: listing
    for listing
    in (get_random_listing(db, max_listings) for _ in range(max_listing_samples))
}

processor = Processor(session, random_topic)
processor.process()
processor._debug_dump(full=True)
