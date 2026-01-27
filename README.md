# Job Market Analysis Tool

This project provides a complete solution for scraping job listings, storing them in a local database, and analyzing market trends through an interactive dashboard.

## Features

- **Job Scraping**: Automated scraping from LinkedIn (via JobSpy).
- **Data Persistence**: SQLite database storage for session management and job listings.
- **Topic Analysis**: Keyword matching and categorization based on configurable topic concepts.
- **Interactive Dashboard**: Streamlit-based UI for visualizing job market trends, level distribution, and keyword popularity.

## Installation

1.  Clone the repository.
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

### 1. Scrape Jobs
Use the `scraper.py` CLI to fetch job listings.

```bash
# Example: Scrape 50 Python jobs in Brazil
python scraper.py --term "Python Developer" --location "Brazil" --count 50 --title "Python Brazil Session"
```

### 2. Launch Dashboard
Start the web interface to analyze the collected data.

```bash
streamlit run webui.py
```

## Structure

- `scraper.py`: CLI tool for data collection.
- `webui.py`: Streamlit dashboard application.
- `database.py`: Database management (SQLite).
- `text_processor.py`: Core logic for processing and analyzing job data.
- `jobspy_processor.py`: jobspy data format related logic.
- `topic_loader.py`: topic parsing (from json) and loading for dynamic selection.
- `topics/`: Configuration files for keyword concepts (json format).
