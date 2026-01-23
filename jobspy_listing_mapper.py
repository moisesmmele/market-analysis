from listings import Listing
import pandas as pd
import json

class JobspyListingMapper:

    @staticmethod
    def from_df(df: pd.DataFrame) -> list[Listing]:
        """Using JobSpy format: Converts a DataFrame to a list of Listings."""
        records: list[dict] = df.to_dict('records')
        listings: list[Listing] = [JobspyListingMapper.from_dict(record) for record in records]
        return listings

    @staticmethod
    def from_dict(data: dict) -> Listing:
        """Using JobSpy format: Converts a dictionary to a Listing."""
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
        """Using JobSpy format: converts a list of Listings to a DataFrame."""
        if not listings:
            return pd.DataFrame()
            
        data: list[dict] = [json.loads(listing.raw_data) for listing in listings]
        return pd.DataFrame(data)