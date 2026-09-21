from datetime import datetime, timezone, timedelta
import io
import os

from zoneinfo import ZoneInfo

import pandas as pd
import requests
import boto3

from airflow.decorators import dag, task


# ============================================================
# Configuration
# ============================================================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

GBFS_DISCOVERY_URL = (
    "https://eu.ftp.opendatasoft.com/star/gbfs/gbfs.json"
)


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="velostar_to_minio",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["transport", "velostar", "gbfs"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
)
def velostar_to_minio():

    @task
    def fetch_and_store():

        # ----------------------------------------------------
        # Étape 1 : fichier de découverte
        # ----------------------------------------------------
        discovery_response = requests.get(
            GBFS_DISCOVERY_URL,
            timeout=30
        )
        discovery_response.raise_for_status()

        discovery = discovery_response.json()

        # ----------------------------------------------------
        # Étape 2 : découverte dynamique de station_status
        # ----------------------------------------------------
        data = discovery.get("data", {})
        feeds = data.get("fr", {}).get("feeds", [])

        if not feeds:
            raise ValueError(
                "Impossible de trouver la liste des flux GBFS."
            )

        status_feed = next(
            (
                feed for feed in feeds
                if feed.get("name") == "station_status"
            ),
            None
        )

        if status_feed is None:
            raise ValueError(
                "Le flux 'station_status' est introuvable."
            )

        status_url = status_feed["url"]

        # ----------------------------------------------------
        # Étape 3 : récupération des stations
        # ----------------------------------------------------
        status_response = requests.get(
            status_url,
            timeout=30
        )
        status_response.raise_for_status()

        status_json = status_response.json()

        stations = (
            status_json
            .get("data", {})
            .get("stations", [])
        )

        if not stations:
            raise ValueError(
                "Aucune station trouvée dans station_status."
            )

        # ----------------------------------------------------
        # Étape 4 : transformation en DataFrame
        # ----------------------------------------------------
        df = pd.DataFrame(stations)

        # ----------------------------------------------------
        # Étape 5 : horodatage de l'ingestion
        # ----------------------------------------------------

        paris_tz = ZoneInfo("Europe/Paris")
        ingested_at = datetime.now(paris_tz)

        df["ingested_at"] = ingested_at.isoformat()

        ingest_date = ingested_at.strftime("%Y-%m-%d")
        ingest_hour = ingested_at.strftime("%H")

        # ----------------------------------------------------
        # Étape 6 : conversion en Parquet
        # ----------------------------------------------------
        buffer = io.BytesIO()

        df.to_parquet(
            buffer,
            index=False
        )

        buffer.seek(0)

        # ----------------------------------------------------
        # Étape 7 : connexion à MinIO
        # ----------------------------------------------------
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_KEY,
            aws_secret_access_key=MINIO_SECRET,
        )

        # ----------------------------------------------------
        # Étape 8 : construction du chemin
        # ----------------------------------------------------
        key = (
            f"velostar/station_status/"
            f"ingest_date={ingest_date}/"
            f"ingest_hour={ingest_hour}/"
            f"{ingested_at.strftime('%Y%m%dT%H%M%S')}.parquet"
        )

        # ----------------------------------------------------
        # Étape 9 : écriture dans le Data Lake
        # ----------------------------------------------------
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=buffer.getvalue(),
        )

        print(
            f"{len(df)} stations enregistrées → "
            f"s3://{MINIO_BUCKET}/{key}"
        )

    fetch_and_store()


velostar_to_minio()