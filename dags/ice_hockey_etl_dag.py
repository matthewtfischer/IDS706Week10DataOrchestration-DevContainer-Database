from __future__ import annotations
import csv
import os
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task
from psycopg2 import Error as DatabaseError
from airflow.providers.postgres.hooks.postgres import PostgresHook

OUTPUT_DIR = "/opt/airflow/data"
TARGET_TABLE = "hockey_stats"

default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="ice_hockey_etl",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
) as dag:
    
    @task()
    def read_csv(filename: str) -> str:
        filepath = os.path.join(OUTPUT_DIR, filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"{filepath} not found!")
        return filepath

    games_file = read_csv("games.csv")
    player_stats_file = read_csv("player_stats.csv")
    players_file = read_csv("players.csv")
    teams_file = read_csv("teams.csv")

    @task()
    def merge_data(player_stats_path, players_path, games_path, teams_path) -> str:
        def read_csv_to_dict(path):
            with open(path, newline="", encoding="utf-8") as f:
                return list(csv.DictReader(f))

        player_stats = read_csv_to_dict(player_stats_path)
        players = read_csv_to_dict(players_path)
        games = read_csv_to_dict(games_path)
        teams = read_csv_to_dict(teams_path)

        players_dict = {p["player_id"]: p for p in players}
        games_dict = {g["game_id"]: g for g in games}
        teams_dict = {t["team_id"]: t for t in teams}

        merged_data = []
        for ps in player_stats:
            player = players_dict.get(ps["player_id"])
            game = games_dict.get(ps["game_id"])
            team = teams_dict.get(player["team_id"]) if player else None
            if not player or not game:
                continue
            merged_data.append({
                "player_id": ps["player_id"],
                "first_name": player["first_name"],
                "last_name": player["last_name"],
                "nickname": player.get("nickname", ""),
                "position": player["position"],
                "team_name": team["team_name"] if team else "",
                "conference": team["conference"] if team else "",
                "game_id": ps["game_id"],
                "game_date": game["game_date"],
                "home_team_id": game["home_team_id"],
                "away_team_id": game["away_team_id"],
                "home_score": game["home_score"],
                "away_score": game["away_score"],
                "goals": ps["goals"],
                "assists": ps["assists"],
                "penalty_minutes": ps["penalty_minutes"]
            })

        merged_path = os.path.join(OUTPUT_DIR, "merged_hockey_data.csv")
        with open(merged_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=merged_data[0].keys())
            writer.writeheader()
            writer.writerows(merged_data)
        return merged_path

    merged_file = merge_data(player_stats_file, players_file, games_file, teams_file)

    @task()
    def load_csv_to_pg(conn_id: str, csv_path: str, table: str = TARGET_TABLE) -> int:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = [tuple(r.get(col, "") for col in fieldnames) for r in reader]

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        {', '.join([f'{col} TEXT' for col in fieldnames])}
                    );
                """
                cur.execute(create_sql)
                insert_sql = f"""
                    INSERT INTO {table} ({', '.join(fieldnames)})
                    VALUES ({', '.join(['%s']*len(fieldnames))});
                """
                cur.executemany(insert_sql, rows)
                conn.commit()
            return len(rows)
        except DatabaseError as e:
            conn.rollback()
            print(f"Error: {e}")
            return 0
        finally:
            conn.close()

    load_task = load_csv_to_pg(conn_id="Postgres", csv_path=merged_file)
    @task()
    def clear_folder(folder_path=OUTPUT_DIR):
        for f in os.listdir(folder_path):
            fp = os.path.join(folder_path, f)
            try:
                if os.path.isfile(fp):
                    os.remove(fp)
                elif os.path.isdir(fp):
                    shutil.rmtree(fp)
            except Exception as e:
                print(f"Failed to delete {fp}: {e}")

    clean_task = clear_folder()
    load_task >> clean_task