# Project: Data Pipeline for Sentiment Analysis and Movie Insights

## Overview
This project implements a data pipeline for scraping sentiment data from websites and analyzing movie-related data. It uses Apache Airflow for orchestration, Selenium for web scraping, and Apache Spark with Delta Lake for data processing and storage.

## Features
- **Sentiment Analysis**: Scrapes sentiment data from websites like YourStory and Finshots.
- **Movie Insights**: Processes movie data to generate insights such as:
  - Mean age of users by occupation.
  - Top 20 highest-rated movies.
  - Top genres rated by users.
  - Similar movies based on user ratings.
- **Orchestration**: Uses Airflow DAGs to schedule and manage tasks.
- **Delta Lake**: Stores processed data with support for versioning and efficient updates.

## Project Structure
```
├── airflow/
│   ├── dags/
│   │   ├── movie-info.py
│   │   ├── scrape-info.py
├── code/
│   ├── python_project/
│   │   ├── scrape_sentiments/
│   │   │   ├── scraper.py
│   │   │   ├── main.py
│   │   │   ├── config.py
│   │   ├── deal_with_movies/
│   │   │   ├── main.py
│   │   │   ├── download.sh
├── data/
│   ├── movie-100k/
│   ├── movie-gold/
│   ├── sentiment-info/
├── deployment/
│   ├── airflow/
│   ├── python/
├── docker-compose.yml
├── setup.sh
```

## Prerequisites
- Docker and Docker Compose
- Python 3.8+
- Apache Airflow
- Apache Spark

## Setup
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. Run the setup script to initialize the environment:
   ```bash 
   bash setup.sh
   ```

3. Access the Airflow webserver at [http://localhost:8080](http://localhost:8080).

## Usage
### Airflow DAGs
- **scrape-info**: Scrapes sentiment data from websites.
- **movie-info**: Processes movie data and generates insights.

### Running Tasks
1. Trigger the `scrape-info` DAG to scrape sentiment data.
2. Trigger the `movie-info` DAG to process movie data and generate insights.

### Command-Line Interface
#### Scrape Sentiments
```bash
python3 code/python_project/scrape_sentiments/main.py \
  --date "2025-04-24" \
  --delta-table-path "/app/data/sentiment-info" \
  --website-to-scrape "yourstory"
```
#### Process Movie Data
```bash
python3 code/python_project/deal_with_movies/main.py \
  --task "mean_age" \
  --delta-table-path "/app/data/movie-gold" \
  --base-data-path "/app/data/movie-100k-u/ml-100k"
```

## Key Files
- **`movie-info.py`**: Airflow DAG for processing movie data.
- **`scrape-info.py`**: Airflow DAG for scraping sentiment data.
- **`scraper.py`**: Web scraping logic for YourStory and Finshots.
- **`main.py`**: Entry point for scraping and processing tasks.
- **`download.sh`**: Script for downloading and verifying movie data.

## License
This project is licensed under the MIT License.