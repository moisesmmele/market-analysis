from jobspy_normalizer import JobspyNormalizer
from jobspy import scrape_jobs
from database import Database
from session import Session
from typing import List, Set
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def scrape_single_term(term: str, location: str, count: int) -> pd.DataFrame:
    """Scrape jobs for a single term - designed to run in parallel."""
    print(f"Scraping '{term}' in '{location}' ({count} results)...")
    df = scrape_jobs(
        site_name=DEFAULT_SITES,
        search_term=term,
        location=location,
        results_wanted=count,
        linkedin_fetch_description=True
    )
    print(f"✓ Completed '{term}' - found {len(df)} listings")
    return df


def scrape(args):
    """
    Scrape jobs based on the provided arguments - NOW WITH PARALLEL EXECUTION!

    Three modes:
    1. Basic (no args): uses default term 'developer' and location 'Brasil'
    2. Comprehensive (--comp): scrapes all predefined terms IN PARALLEL
    3. Custom: uses user-provided terms and settings IN PARALLEL
    """
    print("Starting parallel scrape...")

    terms = get_search_terms(args)
    all_dfs: List[pd.DataFrame] = []

    # Determine number of workers based on number of terms
    # Using min to avoid overwhelming the system
    max_workers = min(args.workers, len(terms))

    print(f"Using {max_workers} parallel workers for {len(terms)} terms")

    # Use ThreadPoolExecutor for parallel scraping
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all scraping tasks
        future_to_term = {
            executor.submit(scrape_single_term, term, args.location, args.count): term
            for term in terms
        }

        # Collect results as they complete
        for future in as_completed(future_to_term):
            term = future_to_term[future]
            try:
                df = future.result()
                all_dfs.append(df)
            except Exception as exc:
                print(f"'{term}' generated an exception: {exc}")

    # merge dataframes and dedupe using df['id']
    combined_df = pd.concat(all_dfs, ignore_index=True)
    clean_df = combined_df.drop_duplicates(subset=['id'], keep='first')

    print(f"\nTotal listings scraped: {len(combined_df)}")
    print(f"Unique listings after deduplication: {len(clean_df)}")

    session_meta = {
        "tool": "jobspy",
        "term": list(terms),
        "location": args.location,
        "sites": DEFAULT_SITES,
        "count": args.count,
        "parallel_workers": max_workers,
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

    parser = argparse.ArgumentParser(description="Job Scraper CLI - NOW WITH PARALLEL EXECUTION!")
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
    parser.add_argument("--workers", type=int, default=2,
                        help="Number of parallel workers (default: 2)")

    args = parser.parse_args()

    # if count is default and comp is true, override it
    if args.comp and args.count == 100:
        args.count = 200

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