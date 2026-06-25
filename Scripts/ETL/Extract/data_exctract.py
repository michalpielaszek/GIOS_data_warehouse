from pathlib import Path
from zipfile import ZipFile
from io import BytesIO

import pandas as pd

# zapisuje bo zapomne
# EPSG:4326 - klasyczne współrzędne kątowe (stopnie, minuty, sekundy itp)
# EPSG:2180 - współrzędne zapisywane już jako konkretne metry 
def transform_epsg4326_to_epsg2180(lat: float, lon: float):
    from pyproj import Transformer # do przeliczenia EPSG:4326 -> EPSG:2180

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:2180", always_xy=True)
    x, y = transformer.transform(lon, lat)

    return x, y

def extract_data_from_GUGiK_API(lat: float, lon: float):
    import requests
    from pyproj import Transformer # do przeliczenia EPSG:4326 -> EPSG:2180

    x, y = transform_epsg4326_to_epsg2180(lat, lon)

    url = "https://uldk.gugik.gov.pl/"
    params = {
        "request": "GetCommuneByXY",
        "xy": f"{x},{y}",
        "result": "teryt,parcel",
    }

    response = requests.get(url, params=params, timeout=20)

    #print(response.url)
    #print(response.status_code)
    #print(response.text)

    return response

from pathlib import Path
import pandas as pd


def parse_uldk_response_to_terc_and_save(
    response,
    kod_stacji: str,
    wgs84_fi_n,
    wgs84_lambda_e,
    cache_path: Path,
):
    text = response.text.strip()
    lines = text.splitlines()

    status = None
    response_text = text
    woj = None
    powiat = None
    gmina = None
    rodzaj_gminy = None
    error = None

    if len(lines) == 0:
        status = "ERROR"
        error = "Pusta odpowiedź z ULDK"

    else:
        status_code_uldk = lines[0].strip()

        if status_code_uldk != "0":
            status = "ERROR"
            error = response_text

        else:
            try:
                status = "OK"

                result = lines[1].strip()

                teryt = result.split("|")[0]
                jednostka = teryt.split(".")[0]

                main, rodzaj_gminy = jednostka.split("_")

                woj = main[0:2]
                powiat = main[2:4]
                gmina = main[4:6]

            except Exception as exc:
                status = "ERROR"
                error = f"Błąd parsowania odpowiedzi: {exc}"

    cache_record = {
        "kod_stacji": kod_stacji,
        "wgs84_fi_n": wgs84_fi_n,
        "wgs84_lambda_e": wgs84_lambda_e,
        "http_status_code": response.status_code,
        "status": status,
        "response_text": response_text,
        "woj": woj,
        "powiat": powiat,
        "gmina": gmina,
        "rodzaj_gminy": rodzaj_gminy,
        "error": error,
    }

    cache_path.parent.mkdir(parents=True, exist_ok=True)

    cache_df = pd.DataFrame([cache_record])

    if cache_path.exists():
        cache_df.to_csv(
            cache_path,
            sep=";",
            index=False,
            encoding="utf-8-sig",
            mode="a",
            header=False,
        )
    else:
        cache_df.to_csv(
            cache_path,
            sep=";",
            index=False,
            encoding="utf-8-sig",
            mode="w",
            header=True,
        )

    if status == "OK":
        return woj, powiat, gmina, rodzaj_gminy

    return None



def read_xlsx_from_file(path: Path) -> dict[str, pd.DataFrame]:
    """
    Czyta pojedynczy plik XLSX.
    Zwraca słownik:
        nazwa_arkusza -> surowy DataFrame

    header=None jest bardzo ważne:
    nie pozwalamy pandasowi zgadywać nagłówków.
    Chcemy zobaczyć plik dokładnie takim, jaki jest.
    """
    sheets = pd.read_excel(
        path,
        sheet_name=None,
        header=None,
        engine="openpyxl"
    )
    return sheets


def read_xlsx_from_zip(zip_path: Path, max_files: int | None = None) -> dict[str, pd.DataFrame]:
    """
    Czyta pliki XLSX znajdujące się wewnątrz ZIP-a.
    Zwraca słownik:
        "zip_name/xlsx_name/sheet_name" -> DataFrame
    """
    result = {}

    with ZipFile(zip_path, "r") as zip_file:
        xlsx_names = [
            name for name in zip_file.namelist()
            if name.lower().endswith(".xlsx")
        ]

        if max_files is not None:
            xlsx_names = xlsx_names[:max_files]

        for xlsx_name in xlsx_names:
            with zip_file.open(xlsx_name) as file:
                content = BytesIO(file.read())

                sheets = pd.read_excel(
                    content,
                    sheet_name=None,
                    header=None,
                    engine="openpyxl"
                )

                for sheet_name, df in sheets.items():
                    key = f"{zip_path.name}/{xlsx_name}/{sheet_name}"
                    result[key] = df

    return result


def load_raw_excels(paths: list[str | Path], max_files_per_zip: int | None = None) -> dict[str, pd.DataFrame]:
    """
    Przyjmuje listę ścieżek do XLSX albo ZIP.
    Zwraca słownik:
        źródło -> surowy DataFrame
    """
    result = {}

    for path in paths:
        path = Path(path)

        if not path.exists():
            print(f"[WARNING] Plik nie istnieje: {path}")
            continue

        if path.suffix.lower() == ".xlsx":
            sheets = read_xlsx_from_file(path)

            for sheet_name, df in sheets.items():
                key = f"{path.name}/{sheet_name}"
                result[key] = df

        elif path.suffix.lower() == ".zip":
            sheets = read_xlsx_from_zip(path, max_files=max_files_per_zip)
            result.update(sheets)

        else:
            print(f"[WARNING] Pomijam nieobsługiwany plik: {path}")

    return result


def show_preview(raw_data: dict[str, pd.DataFrame], rows: int = 10, cols: int = 8) -> None:
    """
    Pokazuje podstawowe informacje o każdym wczytanym arkuszu.
    """
    for source, df in raw_data.items():
        print("\n")
        print("=" * 120)
        print(f"ŹRÓDŁO: {source}")
        print(f"ROZMIAR: {df.shape[0]} wierszy x {df.shape[1]} kolumn")
        print("-" * 120)
        print(df.iloc[:rows, :cols])
        print()

def find_input_files(folder: str | Path, recursive: bool = False) -> list[Path]:
    """
    Znajduje wszystkie pliki XLSX i ZIP w podanym folderze.

    recursive=False:
        szuka tylko bezpośrednio w folderze

    recursive=True:
        szuka też w podfolderach
    """
    folder = Path(folder)

    if not folder.exists():
        raise FileNotFoundError(f"Folder nie istnieje: {folder}")

    if not folder.is_dir():
        raise NotADirectoryError(f"To nie jest folder: {folder}")

    pattern = "**/*" if recursive else "*"

    files = [
        path for path in folder.glob(pattern)
        if path.is_file()
        and path.suffix.lower() in [".xlsx", ".zip"]
        and not path.name.startswith("~$")
    ]

    return sorted(files)

def load_excel_sheets(file_path: str | Path) -> dict[str, pd.DataFrame]:
    file_path = Path(file_path)

    return pd.read_excel(
        file_path,
        sheet_name=None,
        engine="openpyxl"
    )