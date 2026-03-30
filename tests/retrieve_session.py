from persistence import Database

import json

database = Database()

session = database.get_session(2)

subs_data = {}
for subs in session.subsessions.values():
    subs_data[subs.id] = {
        "session_id": subs.session_id,
        "subsession_id": subs.id,
        "start_date": subs.start_date.strftime("%Y-%m-%d"),
        "finish_date": subs.finish_date.strftime("%Y-%m-%d"),
        "status": subs.status,
        "term": subs.search_term,
        "listings": len(subs.listings),
    }

data = {
    "id": session.id,
    "title": session.title,
    "description": session.description,
    "location": session.location,
    "provider": session.provider,
    "platform": session.platform,
    "date": session.date.strftime("%Y-%m-%d"),
    "finished": session.finish_date.strftime("%Y-%m-%d"),
    "meta": session.meta,
    "subsessions": len(session.subsessions),
    "subs_data": subs_data,
}

print(json.dumps(data, indent=2))