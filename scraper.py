from jobspy_normalizer import JobspyNormalizer
from jobspy import scrape_jobs
from database import Database
from session import Session
from typing import List, Set
import pandas as pd

TERMS: dict = {
    "FRONTEND": ['frontend', 'front-end', 'front end'],
    "BACKEND": ['backend', 'back-end', 'back end'],
    "GENERAL": ['developer', 'desenvolvedor', 'desenvolvedora', 'desenvolvedor(a)',
                'programador', 'programadora', 'programador(a)'],
    "FANCY": ['software engineer', 'engenheiro de software'],
}

DEFAULT_SITES = ["linkedin"]


def get_search_terms(args) -> Set[str]:
    if args.comp:
        all_terms = set()
        for values in TERMS.values():
            all_terms.update(values)
        return all_terms
    else:
        return {term.strip() for term in args.term.split(',')}


def scrape(args):
    """
    Scrape jobs based on the provided arguments.

    Three modes:
    1. Basic (no args): uses default term 'developer' and location 'Brasil'
    2. Comprehensive (--comp): scrapes all predefined terms
    3. Custom: uses user-provided terms and settings
    """
    print("Starting scrape...")

    terms = get_search_terms(args)
    all_dfs: List[pd.DataFrame] = []

    for term in terms:
        print(f"Scraping '{term}' in '{args.location}' ({args.count} results)...")
        df = scrape_jobs(
            site_name=DEFAULT_SITES,
            search_term=term,
            location=args.location,
            results_wanted=args.count,
            linkedin_fetch_description=True
        )
        all_dfs.append(df)

    # merge dataframes and dedupe using df['id']
    combined_df = pd.concat(all_dfs, ignore_index=True)
    clean_df = combined_df.drop_duplicates(subset=['id'], keep='first')

    session_meta = {
        "tool": "jobspy",
        "term": list(terms),
        "location": args.location,
        "sites": DEFAULT_SITES,
        "count": args.count,
    }

    return clean_df, session_meta


def create_session_title(args) -> str:
    """Generate an appropriate session title based on arguments."""
    if args.title:
        return args.title
    elif args.comp:
        return "Full scrape - all terms"
    else:
        return f"{args.term} - {args.location}"

def create_session_description(args) -> str:
    """Generate an appropriate session description based on arguments."""
    if args.description:
        return args.description
    elif args.comp:
        return "This session was run using the --comp flag. It scrapes for all possible generic terms related to software development."
    else:
        return ""


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Job Scraper CLI")
    parser.add_argument("--term", type=str, default='developer',
                        help="Search term(s), comma-separated (e.g., 'Python Developer, Java Developer')")
    parser.add_argument("--location", type=str, default='Brasil',
                        help="Location (e.g., 'Brasil or São Paulo')")
    parser.add_argument("--count", type=int, default=100,
                        help="Number of jobs to scrape per term")
    parser.add_argument("--title", type=str, default="",
                        help="Custom title for the session")
    parser.add_argument("--description", type=str, default="",
                        help="Custom description for the session")
    parser.add_argument("--comp", action='store_true',
                        help="Comprehensive scrape using all predefined terms")

    args = parser.parse_args()

    # if count is default and comp is true, override it
    if args.comp and args.count == 100:
        args.count = 1000

    title = create_session_title(args)
    description = create_session_description(args)

    session = Session(title)
    session.description = description
    session.start()

    listings_df, meta = scrape(args)
    session.meta = meta
    session.finish()
    session.listings = JobspyNormalizer.from_df(listings_df)

    # Save to database
    db = Database()
    session_id: int = db.save_session(session)
    db.conn.close()
    print(f"Successfully scraped {len(listings_df)} unique listings for session {session_id}.")