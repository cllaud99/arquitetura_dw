import re
from pathlib import Path
from typing import List
import duckdb


def filter_files_by_year(folder_path: Path, min_year: int = 2020) -> List[Path]:
    """
    Filtra arquivos CSV com padrão 'Acessos_Banda_Larga_Fixa_{ano}.csv' e retorna os com ano > min_year.

    Args:
        folder_path (Path): Pasta com os arquivos extraídos.
        min_year (int): Ano mínimo a considerar.

    Returns:
        List[Path]: Lista de arquivos com ano > min_year.
    """
    pattern = re.compile(r"Acessos_Banda_Larga_Fixa_(\d{4})\.csv", re.IGNORECASE)
    valid_files = []

    for file in folder_path.glob("*.csv"):
        match = pattern.match(file.name)
        if match:
            year = int(match.group(1))
            if year > min_year:
                print(f"[INFO] Arquivo aceito: {file.name} (ano={year})")
                valid_files.append(file)
            else:
                print(f"[DEBUG] Arquivo descartado: {file.name} (ano={year})")
        else:
            print(f"[WARNING] Nome não compatível com o padrão: {file.name}")

    return valid_files


def export_files_to_parquet(files: List[Path], output_folder: Path = Path("data/parquets"), chunk_size: int = 1_500_000) -> None:
    """
    Exporta arquivos CSV para vários arquivos Parquet de até `chunk_size` linhas cada, usando DuckDB.

    Args:
        files (List[Path]): Lista de arquivos CSV para processar.
        output_folder (Path): Pasta onde salvar os arquivos Parquet.
        chunk_size (int): Número máximo de linhas por arquivo Parquet gerado.
    """
    output_folder.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    for file in files:
        try:
            # Descobre total de linhas do CSV via DuckDB
            total_rows = con.execute(f"""
                SELECT COUNT(*) FROM read_csv_auto('{file}', sep=';', decimal_separator=',')
            """).fetchone()[0]

            print(f"[INFO] Processando {file.name}, total de linhas: {total_rows}")

            num_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size != 0 else 0)

            for chunk_idx in range(num_chunks):
                offset = chunk_idx * chunk_size
                output_file = output_folder / f"{file.stem}_part{chunk_idx + 1}.parquet"

                con.execute(f"""
                    COPY (
                        SELECT * FROM read_csv_auto('{file}', sep=';', decimal_separator=',')
                        LIMIT {chunk_size} OFFSET {offset}
                    ) TO '{output_file}' (FORMAT 'parquet')
                """)
                print(f"[INFO] Exportado parte {chunk_idx + 1}/{num_chunks} para {output_file}")

        except Exception as e:
            print(f"[ERROR] Erro exportando arquivo {file.name}: {e}")

    con.close()

if __name__ == "__main__":
    data_folder = Path('data/processed')
    files_to_process = filter_files_by_year(data_folder, min_year=2020)
    export_files_to_parquet(files_to_process)
