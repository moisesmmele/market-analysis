# 💀 Job Market Analysis

A Python-based tool for scraping, storing, and analyzing job market data. It scrapes job listings (primarily from LinkedIn via `jobspy`), stores them in a local SQLite database, and provides a Streamlit dashboard for visualizing insights, such as keyword frequency by job level.

## Features

-   **Job Scraping**: Scrape job listings using the [JobSpy](https://github.com/cullenwatson/JobSpy) library.
-   **Data Persistence**: Store scrape sessions and raw job data in a local SQLite database.
-   **Data Analysis**: Analyze job descriptions to count keywords and bigrams based on predefined topics.
-   **Visualization**: Interactive Streamlit dashboard to view analysis results and trends.
-   **Topic Configuration**: Easy to extend keyword tracking by adding JSON topic files.

## Installation

1.  Clone the repository.
2.  Create a virtual environment (optional but recommended):
    ```bash
    python -m venv .venv
    # Windows
    .venv\Scripts\activate
    # Linux/Mac
    source .venv/bin/activate
    ```
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

### 1. Scraping Jobs

Use the `scraper.py` CLI script to scrape jobs and save them to the database.

**Basic usage:**
```bash
python scraper.py --term "Python Developer" --location "Remote" --count 50
```

**Arguments:**
-   `--term`: Search term (e.g., 'Python Developer', 'Data Scientist'). Default: 'php'.
-   `--location`: Location (e.g., 'San Francisco', 'Brazil'). Default: 'Resende'.
-   `--count`: Number of jobs to scrape. Default: 100.
-   `--country`: Country context for scraping. Default: 'Brazil'.
-   `--title`: (Optional) Custom title for the session. Defaults to: `{term} - {location} - {timestamp}`.

### 2. Viewing Results

Launch the web interface to analyze saved sessions.

```bash
streamlit run webui.py
```

-   Select a session from the sidebar.
-   Click **Load Session**.
-   View bar charts showing the frequency of keywords (from your `topics/` configuration) found in the job descriptions.

## Project Structure

-   **`scraper.py`**: CLI entry point for scraping and saving data.
-   **`webui.py`**: Streamlit application for the user interface.
-   **`database.py`**: Handles SQLite interactions (schema provisioning, saving/retrieving sessions).
-   **`jobspy_processor.py`**: Business logic for analyzing job descriptions against topics and job levels.
-   **`jobspy_normalizer.py`**: Standardizes incoming data from different sources into a common format. This decouples the core application from specific scraper data structures, making it easier to adapt to upstream changes.
-   **`text_processor.py`**: Utilities for text cleaning, bigram extraction, and keyword matching.
-   **`config.py`**: Central configuration (paths to data and topics).
-   **`topics/`**: Directory containing JSON files defining keywords to track (e.g., `frontend.json`, `backend.json`).

## Configuration

To add new keywords to track, create or edit a JSON file in the `topics/` directory.

**Example Topic File (`topics/python.json`):**
```json
{
  "title": "Python Ecosystem",
  "description": "Python libraries and frameworks",
  "aliases": {
    "django": ["django framework"],
    "fastapi": ["fast api"]
  },
  "words": [
    "django",
    "flask",
    "fastapi",
    "pandas",
    "numpy"
  ]
}
```
