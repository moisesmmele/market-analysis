import pandas as pd
import re
import unicodedata
from collections import Counter
from Session import Session
from keyword_sets import stopwords, backend_keywords, frontend_keywords, language_keywords, general_keywords, seniority

# Data Processor Class for Pandas DataFrames and JobSpy data

class JobspyProcessor:

    df: pd.DataFrame

    def load_from_session(self, session: Session):
        self.df = pd.DataFrame(session.listings)

    def append_to_session(self, session: Session):
        pass


def process_dataframe(df):
    """
    Enriches the dataframe with processed columns:
    - 'seniority_found': List of seniorities found
    - 'keywords_backend': List of backend keywords found
    - 'keywords_frontend': List of frontend keywords found
    - 'keywords_language': List of language keywords found
    - 'keywords_general': List of general keywords found
    """
    if df.empty or 'description' not in df.columns:
        return df

    # We use apply. For very large datasets we might optimize, but this is fine for typical sessions.
    
    # Seniority
    df['seniority_found'] = df['description'].apply(get_seniority_list)
    
    # Keywords
    # We can perform the clean once per row and then match
    def enrich_row(desc):
        if not isinstance(desc, str):
            return pd.Series([[], [], [], []])
        
        kw_dict = get_keywords_from_text(desc)
        return pd.Series([
            kw_dict['backend'],
            kw_dict['frontend'],
            kw_dict['language'],
            kw_dict['general']
        ])

    df[['keywords_backend', 'keywords_frontend', 'keywords_language', 'keywords_general']] = df['description'].apply(enrich_row)
    
    return df

def aggregate_keywords(df):
    """
    Aggregates keywords from the processed dataframe columns.
    Returns a tuple of DataFrames for visualization: (backend, frontend, language, general, seniority)
    """
    if df.empty:
        return tuple([pd.DataFrame(columns=["Termo", "Freq"])] * 4 + [pd.DataFrame(columns=["Nível", "Contagem"])])

    def get_counts(col_name):
        all_items = [item for sublist in df[col_name] for item in sublist]
        return pd.DataFrame(Counter(all_items).most_common(100), columns=["Termo", "Freq"])

    df_backend = get_counts('keywords_backend')
    df_frontend = get_counts('keywords_frontend')
    df_language = get_counts('keywords_language')
    df_general = get_counts('keywords_general')
    
    # Seniority
    all_sen = [item for sublist in df['seniority_found'] for item in sublist]
    df_sen = pd.DataFrame(Counter(all_sen).most_common(), columns=["Nível", "Contagem"])

    return df_backend, df_frontend, df_language, df_general, df_sen