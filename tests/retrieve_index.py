import json

from app.persistence import Database

db = Database()

index = db.get_index()
print(json.dumps(index, indent=2))