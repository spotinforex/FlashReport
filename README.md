# Flashreport

**Flashreport** — A News Intelligence Gathering Platform

An investigative intelligence agent designed to monitor, scrape, filter, and analyze news content from multiple sources. Flashreport helps uncover early indicators in news coverage and delivers actionable insights via integrated data pipelines.

## Overview

Flashreport is built to gather, preprocess, and distribute news intelligence from a variety of publishers. The platform supports configurable scraping adapters, preprocessing workflows, news filtering logic, and secure access via authentication components.

## Key Capabilities

- News scraping from multiple sources using adapter-based collectors
- Data preprocessing and normalization before filtering
- Intelligent filtering and classification for news relevance
- Support for fast API-driven workflows and Telegram integration
- Database communication components for persistence and analytics

## Project Structure

- `main.py` — Application entry point
- `config.py` — Configuration loader and environment settings
- `Algorithm/` — Filtering and clustering logic
  - `cluster.py` — Clustering operations
  - `filter.py` — Core filtering implementation
  - `gemini_filter.py` — Extended filtering logic
- `Auth/` — Authentication and verification layer
  - `app.py` — Auth service integration
  - `db_communicator.py` — Database access helper
  - `verifier.py` — Authentication/verification utilities
- `scrapper/` — Scraping and data ingestion pipeline
  - `database.py` — Database helpers for scraped data
  - `flood_data.py` — Flood monitoring data collector
  - `flood.py` — Flood report scraping logic
  - `preprocessor.py` — Content preprocessing routines
  - `scrapy.py` — Scraping orchestration
  - `telegram.py` — Telegram notification and message adapter
  - `adapter/` — Source-specific scrapers for news publishers
- `tests/` — Unit tests for sources, preprocessing, and filters

## Installation

1. Clone the repository:

```bash
git clone https://github.com/your-org/flashreport.git
cd flashreport
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

> If `requirements.txt` is not present, install dependencies from `pyproject.toml` with `pip install .`.

## Configuration

Copy or create a `.env` file and populate it with environment-specific values such as database credentials, API keys, and service endpoints.

Example keys:

- `DATABASE_URL`
- `SUPABASE_URL`
- `SUPABASE_KEY`
- `TELEGRAM_API_KEY`
- `TELEGRAM_CHAT_ID`

## Running the Application

Launch the app with:

```bash
python main.py
```

If the project exposes a FastAPI service, you can run with Uvicorn:

```bash
uvicorn main:app --reload
```

## Testing

Run the existing test suite with:

```bash
pytest
```

## Contributing

Contributions are welcome. Please follow these guidelines:

1. Open an issue for new features or bugs
2. Create a branch for your changes
3. Add tests for new behavior
4. Submit a pull request with a clear description

## License

This project is distributed under the terms of the license in `LICENSE`.

## Notes

Flashreport is intended as an investigative agent that assists analysts in discovering signal from news noise. It can be extended with new scrapers, filter rules, and intelligence workflows to match specific domains or investigative use cases.
