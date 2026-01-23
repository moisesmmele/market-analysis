from Session import Session
from database import Database
from jobspy import scrape_jobs

def scrape(args):

    sites = ["linkedin"]
    listings_df = scrape_jobs(
        site_name=sites,
        search_term=args.term,
        location=args.location,
        results_wanted=args.count,
        linkedin_fetch_description=True
    )

    meta = {
        "tool": "jobspy",
        "term": args.term,
        "location": args.location,
        "sites": sites,
        "count": args.count,
        "country": args.country
    }
    return listings_df, meta


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Job Scraper CLI")
    parser.add_argument("--term", type=str, default='php', help="Search term (e.g., 'Python Developer')")
    parser.add_argument("--location", type=str, default='Brasil', help="Location (e.g., 'Brazil')")
    parser.add_argument("--count", type=int, default=50, help="Number of jobs to scrape")
    parser.add_argument("--country", type=str, default="Brazil", help="Country for scraping context")
    parser.add_argument("--title", type=str, default="", help="Title for the session")
    args = parser.parse_args()

    print(f"Starting scrape for '{args.term}' in '{args.location}' ({args.count} results)...")

    if args.title == "":
        title = f"{args.term} - {args.location} - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    else:
        title = args.title

    session = Session(title)
    session.start()
    
    listings_df, meta = scrape(args)
    
    session.meta = meta
    
    session.finish()
    db = Database()
    session.id = db.save_session(session)

    processor = JobspyProcessor()
    for row in listings_df:
        processor.append_to_session(session)
        

    for listing in session.listings:
        db.save_listing(listing)

    print(f"Successfully scraped {len(listings_df)} listings for session {session.id}.")