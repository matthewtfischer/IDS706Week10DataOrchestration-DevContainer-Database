# IDS706Week10DataOrchestration-DevContainer-Database
## Ice Hockey ETL Pipeline (Airflow + PostgreSQL)

This project implements an ETL data pipeline using Apache Airflow and PostgreSQL, designed to process and manage data about ice hockey games, players, and teams.

---

## Project Structure
Week 10 Major Assignment/
├── dags/

│ └── ice_hockey_etl_dag.py # Main Airflow DAG

├── data/

│ ├── games.csv # Raw data extracted from SQLite

│ ├── players.csv

│ ├── teams.csv

│ ├── player_stats.csv

│ ├── merged_hockey_data.csv # Final merged dataset

├── sample_ice_hockey_data.db # Original SQLite database

├── config/

│ └── airflow.cfg

├── logs/ # Airflow logs (auto-generated)

└── requirements.txt

---

## Pipeline Overview

The ETL workflow is managed by Apache Airflow and defined in the DAG `ice_hockey_etl_dag.py`.  
It performs the following key steps:

### 1. Extract
- Connects to a SQLite database (`sample_ice_hockey_data.db`).
- Exports the following tables to CSV files under `data/`:
  - `games`
  - `player_stats`
  - `players`
  - `teams`

### 2. Transform
- Reads the CSV files using pandas.
- Merges them into a single dataset (`merged_hockey_data.csv`) using relational keys such as:
  - `team_id`
  - `player_id`
  - `game_id`
- Performs basic data cleaning and integrity checks (e.g., removing nulls or duplicates).

### 3. Load
- Connects to a PostgreSQL database running inside Docker.
- Loads the merged dataset into a target table (e.g. `ice_hockey_data`) in PostgreSQL.

---

## Docker Setup

The project uses Docker containers for Airflow and Postgres.  

## Database Connection (Airflow UI)

In the Airflow web interface (Admin → Connections), define a connection called Postgres:

| Field | Value |
|-------|-------|
| Connection ID | `Postgres` |
| Connection Type | `Postgres` |
| Host | `db` |
| Database | `airflow_db` |
| Login | `vscode` |
| Password | `vscode` |
| Port | `5432` |
