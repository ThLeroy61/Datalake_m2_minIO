import io
import json
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd

from airflow.datasets import Dataset
from airflow.decorators import dag, task


# ============================================================
# Configuration
# ============================================================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

STATION_STATUS_DATASET = Dataset(
    f"minio://{MINIO_BUCKET}/velostar/station_status/"
)

STATION_STATUS_PREFIX = "velostar/station_status/"
STATION_INFO_KEY = "reference/station_information/station_information.json"
HISTORIQUE_PREFIX = "velomax-historic/"


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="velostar_historique_to_minio",
    schedule=[STATION_STATUS_DATASET],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["transport", "velostar", "gbfs"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
)
def velostar_historique_to_minio():

    @task
    def build_and_store_historique():

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_KEY,
            aws_secret_access_key=MINIO_SECRET,
        )

        # ----------------------------------------------------
        # Étape 1 : lister tous les fichiers station_status
        # ----------------------------------------------------
        paginator = s3.get_paginator("list_objects_v2")
        fichiers = []

        for page in paginator.paginate(
            Bucket=MINIO_BUCKET,
            Prefix=STATION_STATUS_PREFIX,
        ):
            fichiers.extend(
                obj["Key"] for obj in page.get("Contents", [])
            )

        if not fichiers:
            raise ValueError(
                "Aucun fichier station_status trouvé dans MinIO."
            )

        # ----------------------------------------------------
        # Étape 2 : empiler tous les parquets en un historique
        # ----------------------------------------------------
        dataframes = []
        for cle in fichiers:
            obj = s3.get_object(Bucket=MINIO_BUCKET, Key=cle)
            dataframes.append(
                pd.read_parquet(io.BytesIO(obj["Body"].read()))
            )

        historique = pd.concat(dataframes, ignore_index=True)

        # ----------------------------------------------------
        # Étape 3 : enrichir avec les infos stations
        # ----------------------------------------------------
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=STATION_INFO_KEY)
        info_json = json.loads(obj["Body"].read())
        stations_info = pd.DataFrame(info_json["data"]["stations"])

        historique_enrichi = historique.merge(
            stations_info[["station_id", "name", "capacity"]],
            on="station_id",
            how="left",
        )

        # ----------------------------------------------------
        # Étape 4 : conversion en Parquet
        # ----------------------------------------------------
        dernier_ingested_at = historique["ingested_at"].max()
        horodatage = pd.to_datetime(dernier_ingested_at).strftime(
            "%Y%m%dT%H%M%S"
        )

        buffer = io.BytesIO()
        historique_enrichi.to_parquet(buffer, index=False)
        buffer.seek(0)

        # ----------------------------------------------------
        # Étape 5 : écriture dans le Data Lake
        # ----------------------------------------------------
        key = f"{HISTORIQUE_PREFIX}historique_{horodatage}.parquet"

        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=buffer.getvalue(),
        )

        print(
            f"{len(historique_enrichi)} lignes enregistrées → "
            f"s3://{MINIO_BUCKET}/{key}"
        )

    build_and_store_historique()


velostar_historique_to_minio()