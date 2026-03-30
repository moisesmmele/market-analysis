import jobspy
import json

from app.legacy.jobspy_normalizer import JobspyNormalizer
from app.entities import Session, Subsession, Listing
from app.persistence import Database
from app import config

max_workers = 2

search_terms = ["php", "python", "javascript"]
location = "Brazil"
results_wanted_per_term = 20

def scrape_with_jobspy(session: Session, db: Database):
    print("Starting scrape with jobspy...")
    for subsession in session.subsessions.values():
        subsession: Subsession
        print(f"Scraping for: {subsession.search_term}")

        df = jobspy.scrape_jobs(
            site_name=[session.platform],
            search_term=subsession.search_term,
            location=session.location,
            results_wanted=results_wanted_per_term,
            linkedin_fetch_description=True
        )

        print("Finished scraping.")
        for record in json.loads(df.to_json(orient="records")):
            listing = Listing()
            listing.subsession_id = subsession.id
            listing.raw_data = json.dumps(record)
            subsession.append_listing(listing)

        subsession.finish()
        subs_id = db.update_subsession(subsession)

        print(f"Saved subsession {subs_id} to database.")

database = Database()
_session = Session()
_session.title = "Scrape Session Test - Real Data"
_session.description = "A real scrape session for testing purposes"
_session.location = location
_session.meta = {"test": "test"}
_session.provider = config.mappings.provider
_session.platform = config.mappings.platform
for term in search_terms:
    _session.add_subsession(term)

_session.start()

session = database.save_session(_session)

print(f"Session {session.id} crated successfully.")

scrape_with_jobspy(session, database)