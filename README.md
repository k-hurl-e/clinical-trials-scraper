# Clinical Trials Scraper

This project is a Python-based scraper for extracting clinical trials data from ClinicalTrials.gov. The scraper allows users to search for clinical trials based on various criteria and store the results in a PostgreSQL database.

## Features

- Search for clinical trials by medical condition, intervention, and other terms.
- Option to fetch only trials with results.
- Store trial data in a PostgreSQL database.

## Requirements

- Python 3.7+
- PostgreSQL
- Required Python packages (listed in `requirements.txt`)

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/clinical-trials-scraper.git
    cd clinical-trials-scraper
    ```

2. Create and activate a virtual environment:
    ```sh
    python3 -m venv venv
    source venv/bin/activate
    ```

3. Install the required packages:
    ```sh
    pip install -r requirements.txt
    ```

4. Set up the PostgreSQL database and create the necessary tables. Create a `.env` file with your database connection details:

```
DATABASE_URL=postgresql://username:password@localhost:5432/your_db_name
```

## Command-line Arguments
`--condition`: Medical condition to search for.
`--intervention`: Treatment/intervention to search for.
`--other-terms`: Additional search terms to filter trials.
`--max-trials`: Maximum number of trials to fetch.
`--results-only`: Only fetch trials with results.

## Usage

Run the scraper with the desired search parameters:

```sh
python scraper.py --condition "diabetes" --max-trials 100 --results-only
```

## Export

Export to csv or json. Update info in `csvconverter.py` and `jsonconverter.py`.

```sh
python csvconverter.py
python jsonconverter.py
```