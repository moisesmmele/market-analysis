from dynamic_listing import Listing
import pandas as pd
import json

"""Normalization module to convert Jobspy Ingest data into agnostic entities AND entities into jobspy data format"""
class JobspyNormalizer:

    # Field mappings
    # Key: Canonical Name
    # Value: Jobspy Name
    FIELD_MAPS = {
        'location': 'location',
        'company': 'company',
        'job_level': 'job_level',
        'title': 'title',
        'date_posted': 'date_posted',
    }

    @classmethod
    def from_df(cls, df: pd.DataFrame) -> list[Listing]:
        """Converts a DataFrame to a list of Listings."""
        return [cls.from_dict(record) for record in df.to_dict('records')]

    @classmethod
    def from_dict(cls, data: dict) -> Listing:
        """Converts a dictionary to a Listing using Jobspy mappings"""

        mapped_data = {
            canonical: data.get(jobspy_key)
            for canonical, jobspy_key in cls.FIELD_MAPS.items()
        }

        listing = Listing(**mapped_data)
        listing.raw_data = json.dumps(data, default=str) if data else None
        return listing

    @classmethod
    def to_df(cls, listings: list[Listing]) -> pd.DataFrame:
        """converts a list of Listings back to a DataFrame. Uses raw_data from jobspy scrape jobs."""
        return pd.DataFrame([json.loads(listing.raw_data) for listing in listings if listing.raw_data])