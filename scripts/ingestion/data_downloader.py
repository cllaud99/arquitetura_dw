# scripts/ingestion/data_downloader.py

import os
import zipfile
from pathlib import Path

import requests


def download_file(url: str, destination_path: Path) -> Path:
    """
    Baixa um arquivo de uma URL e o salva em um caminho especificado.

    Args:
        url (str): A URL do arquivo a ser baixado.
        destination_path (Path): O caminho completo, incluindo o nome do arquivo,
                                 onde o arquivo deve ser salvo.

    Returns:
        Path: O caminho completo do arquivo salvo se o download for bem-sucedido.

    Raises:
        requests.exceptions.RequestException: Se houver um erro de conexão ou HTTP.
        IOError: Se houver um erro ao escrever o arquivo.
    """
    print(f"Iniciando download do arquivo de: {url}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Levanta um HTTPError para status de erro (4xx ou 5xx)

        # Garante que o diretório de destino exista
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        with open(destination_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Arquivo baixado com sucesso para: {destination_path}")
        return destination_path
    except requests.exceptions.RequestException as e:
        print(f"Erro de rede ou HTTP ao baixar {url}: {e}")
        raise
    except IOError as e:
        print(f"Erro de I/O ao salvar o arquivo em {destination_path}: {e}")
        raise
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante o download: {e}")
        raise


def extract_zip_file(zip_path: Path, destination_dir: Path) -> None:
    """
    Descompacta um arquivo ZIP em um diretório de destino.

    Args:
        zip_path (Path): O caminho completo para o arquivo ZIP.
        destination_dir (Path): O caminho completo para o diretório de destino.

    Raises:
        zipfile.BadZipFile: Se o arquivo ZIP for inválido.
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(destination_dir)
        print(f"Arquivo ZIP descompactado com sucesso para: {destination_dir}")
    except zipfile.BadZipFile as e:
        print(f"Erro ao descompactar o arquivo ZIP: {e}")
        raise


# Para testes locais, fora do Airflow.
if __name__ == "__main__":

    zip_path = "data/raw/acessos_banda_larga_fixa.zip"
    destination_dir = "data/processed"
    extract_zip_file(zip_path, destination_dir)
