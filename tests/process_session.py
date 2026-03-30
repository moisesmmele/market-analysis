from core import Processor
from loaders import TopicLoader
from persistence import Database

database = Database()
topics = TopicLoader.select(all_topics=True)

session = database.get_session(3)

processor = Processor(session, topics)
processor.process()
processor._debug_dump(full=True)
