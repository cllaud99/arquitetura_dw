import os
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import List

from airflow.sdk import dag, task
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scripts.ingestion.data_downloader import download_file
from scripts.preprocessing.duckdb_preprocessor import filter_files_by_year

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ===== CONFIGURAÇÕES =====
BROADBAND_URL = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"
SMP_URL = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/infraestrutura/smp_mun_atendidos.zip"

RAW_FOLDER = Path("/opt/airflow/data/raw")
EXTRACT_FOLDER = Path("/opt/airflow/data/extract")

BROADBAND_ZIP_PATH = RAW_FOLDER / "acessos_banda_larga_fixa.zip"
SMP_ZIP_PATH = RAW_FOLDER / "smp_mun_atendidos.zip"

BROADBAND_FOLDER = EXTRACT_FOLDER / "broadband"
SMP_FOLDER = EXTRACT_FOLDER / "smp"


def normalize_header(header: str) -> str:
    nfkd = unicodedata.normalize("NFKD", header)
    only_ascii = nfkd.encode("ASCII", "ignore").decode("ASCII")
    return only_ascii.lower().replace(" ", "_")


def fix_csv_header(file_path: Path) -> Path:
    """
    Corrige o cabeçalho do CSV para o formato normalizado e, se presente,
    converte vírgulas por ponto na coluna 'velocidade'.

    Args:
        file_path (Path): Caminho do arquivo original CSV.

    Returns:
        Path: Caminho do arquivo temporário corrigido.
    """
    tmp_path = Path("/tmp") / f"tmp_{file_path.name}"
    with file_path.open("r", encoding="utf-8") as original:
        header = original.readline().strip().split(';')
        normalized_header = [normalize_header(col) for col in header]
        velocidade_index = normalized_header.index("velocidade") if "velocidade" in normalized_header else None

        with tmp_path.open("w", encoding="utf-8") as temp:
            temp.write(";".join(normalized_header) + "\n")

            if velocidade_index is None:
                temp.writelines(original)
            else:
                for line in original:
                    values = line.strip().split(';')
                    if len(values) > velocidade_index:
                        values[velocidade_index] = values[velocidade_index].replace(",", ".")
                    temp.write(";".join(values) + "\n")

    return tmp_path


@dag(
    dag_id="migration_anatel_to_postgres_dw_copy_metody",
    description="Migração dos dados da Anatel para PostgreSQL via COPY otimizado",
    schedule=None,
    start_date=datetime(2025, 6, 4),
    catchup=False,
    tags=["anatel", "postgres", "migration"],
)
def migration_anatel_dag():

    @task()
    def download_zip(url: str, zip_path: Path) -> str:
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        download_file(url=url, destination_path=zip_path)
        return str(zip_path)

    @task()
    def extract_zip(zip_path: str, target_folder: str) -> str:
        from scripts.ingestion.data_downloader import extract_zip_file

        target = Path(target_folder)
        target.mkdir(parents=True, exist_ok=True)
        extract_zip_file(Path(zip_path), destination_dir=target)
        return str(target)

    @task()
    def filter_csv_files(folder_path: str, min_year: int | None = None) -> list[str]:
        folder = Path(folder_path)
        files = list(folder.glob("*.csv"))
        if min_year:
            files = filter_files_by_year(folder, min_year=min_year)
        return [str(f) for f in files]

    @task()
    def load_csv_files_to_postgres(csv_files: List[str], table_name: str) -> None:
        """
        Limpa a tabela e carrega os arquivos CSV no Postgres usando COPY com cabeçalhos normalizados.
        """
        pg_hook = PostgresHook(postgres_conn_id="dev-postgres")
        pg_hook.run(f"TRUNCATE TABLE {table_name};")
        print(f"Tabela {table_name} truncada.")

        for file_path_str in csv_files:
            file_path = Path(file_path_str)
            print(f"Processando arquivo {file_path}...")

            tmp_file = fix_csv_header(file_path)

            pg_hook.copy_expert(
                sql=f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ';'",
                filename=str(tmp_file),
            )

            tmp_file.unlink()
            print(f"Arquivo {file_path} carregado com sucesso.")

    # ===== DEFINIÇÃO DO FLUXO DA DAG =====
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    broadband_zip = download_zip(BROADBAND_URL, BROADBAND_ZIP_PATH)
    smp_zip = download_zip(SMP_URL, SMP_ZIP_PATH)

    broadband_extracted = extract_zip(broadband_zip, str(BROADBAND_FOLDER))
    smp_extracted = extract_zip(smp_zip, str(SMP_FOLDER))

    broadband_csv_files = filter_csv_files(broadband_extracted, min_year=2020)
    smp_csv_files = filter_csv_files(smp_extracted)

    load_broadband = load_csv_files_to_postgres(broadband_csv_files, "raw_broadband_copy")
    load_smp = load_csv_files_to_postgres(smp_csv_files, "raw_smp_copy")

    start >> [broadband_zip, smp_zip]
    broadband_zip >> broadband_extracted >> broadband_csv_files >> load_broadband >> end
    smp_zip >> smp_extracted >> smp_csv_files >> load_smp >> end


dag = migration_anatel_dag()
