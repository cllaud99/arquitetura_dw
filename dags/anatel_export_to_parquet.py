import os
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytz
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Airflow
from airflow.sdk import dag, task
from pandas.errors import OutOfBoundsDatetime

from scripts.ingestion.data_downloader import download_file, extract_zip_file
from scripts.preprocessing.duckdb_preprocessor import (
    export_files_to_parquet,
    filter_files_by_year,
)

# Garante que scripts/ está no sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ===== CONFIGURAÇÕES GLOBAIS =====

# URLs dos arquivos ZIP
BROADBAND_URL = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"
SMP_URL = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/infraestrutura/smp_mun_atendidos.zip"

# Diretórios base
RAW_FOLDER = Path("/opt/airflow/data/raw")
PROCESSED_FOLDER = Path("/opt/airflow/data/extract")
PARQUET_OUTPUT_FOLDER = Path("/opt/airflow/data/parquets")

# Caminhos dos arquivos ZIP
BROADBAND_ZIP_PATH = RAW_FOLDER / "acessos_banda_larga_fixa.zip"
SMP_ZIP_PATH = RAW_FOLDER / "smp_mun_atendidos.zip"

# Subpastas específicas para extração
BROADBAND_FOLDER = PROCESSED_FOLDER / "broadband"
SMP_FOLDER = PROCESSED_FOLDER / "smp"


@dag(
    dag_id="migration_anatel_parquet",
    description="Migração dos dados da Anatel para PostgreSQL",
    schedule=None,
    start_date=datetime(2025, 6, 4),
    catchup=False,
    tags=["anatel", "parquet", "migration"],
)
def migration_anatel_dag():

    @task()
    def download_broadband_zip() -> str:
        """
        Faz o download do arquivo de banda larga fixa da Anatel.

        Returns:
            str: Caminho do arquivo .zip salvo.
        """
        BROADBAND_ZIP_PATH.parent.mkdir(parents=True, exist_ok=True)
        download_file(url=BROADBAND_URL, destination_path=BROADBAND_ZIP_PATH)
        return str(BROADBAND_ZIP_PATH)

    @task()
    def download_smp_zip() -> str:
        """
        Faz o download do arquivo de municípios atendidos por SMP da Anatel.

        Returns:
            str: Caminho do arquivo .zip salvo.
        """
        SMP_ZIP_PATH.parent.mkdir(parents=True, exist_ok=True)
        download_file(url=SMP_URL, destination_path=SMP_ZIP_PATH)
        return str(SMP_ZIP_PATH)

    @task()
    def extract_zip(zip_path: str, target_folder: str) -> str:
        """
        Extrai o conteúdo de um arquivo .zip para a pasta de destino.

        Args:
            zip_path (str): Caminho do arquivo zipado.
            target_folder (str): Pasta onde os arquivos serão extraídos.

        Returns:
            str: Caminho da pasta de extração.
        """
        target = Path(target_folder)
        target.mkdir(parents=True, exist_ok=True)
        extract_zip_file(Path(zip_path), destination_dir=target)
        return str(target)

    @task()
    def convert_to_parquet(
        source: str | list[str], output_subfolder: str, apply_filter: bool = False
    ) -> str:
        """
        Converte arquivos CSV para o formato Parquet.

        Args:
            source (str | list[str]): Caminho da pasta com arquivos CSV ou lista de caminhos.
            output_subfolder (str): Nome da subpasta dentro da pasta de saída.
            apply_filter (bool): Se verdadeiro, aplica filtro para arquivos com ano >= 2020.

        Returns:
            str: Caminho da pasta contendo os arquivos Parquet.
        """
        if isinstance(source, str):
            folder = Path(source)
            if apply_filter:
                files = filter_files_by_year(folder, min_year=2020)
            else:
                files = list(folder.glob("*.csv"))
        else:
            files = [Path(p) for p in source]

        output_folder = PARQUET_OUTPUT_FOLDER / output_subfolder
        output_folder.mkdir(parents=True, exist_ok=True)
        export_files_to_parquet(files, output_folder=output_folder, chunk_size=1_000_000_000)
        return str(output_folder)

    # Operadores de controle
    start = EmptyOperator(task_id="inicio_pipeline")
    end = EmptyOperator(task_id="fim_pipeline")

    # Encadeamento das tarefas
    broadband_zip = download_broadband_zip()
    smp_zip = download_smp_zip()

    broadband_extracted = extract_zip(broadband_zip, str(BROADBAND_FOLDER))
    smp_extracted = extract_zip(smp_zip, str(SMP_FOLDER))

    broadband_parquets = convert_to_parquet(
        source=broadband_extracted,
        output_subfolder="broadband",
        apply_filter=True,
    )

    smp_parquets = convert_to_parquet(
        source=smp_extracted,
        output_subfolder="smp",
        apply_filter=False,
    )


    start >> [broadband_zip, smp_zip]

    (
        broadband_zip
        >> broadband_extracted
        >> broadband_parquets
    )

    (
        smp_zip
        >> smp_extracted
        >> smp_parquets
    )

    # Ambos convergem para o end
    [broadband_parquets, smp_parquets] >> end


# Instancia a DAG
dag = migration_anatel_dag()
