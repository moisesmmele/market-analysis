from click import echo

from listing import Listing
import pandas as pd
import json

"""Normalization module to convert Jobspy Ingest data into agnostic entities AND entities into jobspy data format"""
class JobspyNormalizer:

    @staticmethod
    def from_df(df: pd.DataFrame) -> list[Listing]:
        """Converts a DataFrame to a list of Listings."""
        records: list[dict] = df.to_dict('records')
        listings: list[Listing] = [JobspyNormalizer.from_dict(record) for record in records]
        return listings

    @staticmethod
    def from_dict(data: dict) -> Listing:
        """Converts a dictionary to a Listing using Jobspy mappings"""
        listing = Listing()
        listing.location = data.get('location')
        listing.company = data.get('company')
        listing.job_level = data.get('job_level')
        listing.title = data.get('title')
        listing.date_posted = data.get('date_posted')
        
        listing.raw_data = json.dumps(data, default=str)
        return listing

    @staticmethod
    def to_df(listings: list[Listing]) -> pd.DataFrame:
        """converts a list of Listings back to a DataFrame. Uses raw_data from jobspy scrape jobs."""
        if not listings:
            return pd.DataFrame()
            
        data: list[dict] = [json.loads(listing.raw_data) for listing in listings]
        return pd.DataFrame(data)