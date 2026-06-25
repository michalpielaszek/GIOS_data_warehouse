import pandas as pd
import numpy as np
from pathlib import Path

from Scripts.ETL.Extract.data_exctract import (
    load_excel_sheets,
    parse_uldk_response_to_terc_and_save,
    extract_data_from_GUGiK_API,
    transform_epsg4326_to_epsg2180
)
from Scripts.ETL.Transform.clean_columns import prepare_wojewodztwa_df, prepare_numeric_column, prepare_text_column
from Scripts.Database.connection import get_engine
from Scripts.ETL.Load.data_load import (
    insert_records,
    get_id_map,
    get_powiat_terc_map,
    get_gmina_teryt_map,
    get_miejscowosc_woj_name_map,
    get_miejscowosc_simc_map,
    get_norma_map
)
from Scripts.ETL.Transform.terc_transform import (
    load_terc,
    prepare_wojewodztwa_from_terc,
    prepare_powiaty_from_terc,
    prepare_gminy_from_terc,
)

from Scripts.ETL.Transform.clean_columns import (
    prepare_stacje_from_metadata,
    apply_stacje_miejscowosci_corrections
)

from Scripts.ETL.Transform.simc_transform import load_simc, prepare_miejscowosci_from_simc

path_metadata = "Data\Raw\Metadane oraz kody stacji i stanowisk pomiarowych.xlsx"
PATH_TERC = r"Data/Raw/TERC_Urzedowy_2026-06-07.csv"
path_simc = "Data/Raw/SIMC_Urzedowy_2026-06-14.csv"
path_stacje_korekty = Path("Data") / "Temp" / "stacje_korekty.csv"


def run_pipeline():
    # pass
    # metadata_sheets = load_excel_sheets(path_metadata)

    # ark_stacje = metadata_sheets[list(metadata_sheets.keys())[0]]
    # ark_stanowiska = metadata_sheets[list(metadata_sheets.keys())[1]]

    # print(f"Arkusz {list(metadata_sheets.keys())[0]}:")
    # print(ark_stacje.head(10))

    # print(f"Arkusz {list(metadata_sheets.keys())[1]}:")
    # print(ark_stanowiska.head(10))

    # print("="*50)
    # print(ark_stacje.columns)

    # print("="*50)
    # print(ark_stanowiska.columns)

    # print("="*50)
    # prepare_wojewodztwa_df(ark_stanowiska)

    #load_terc_to_db()
    #load_strefy_to_db()
    #load_miejscowosci_to_db()
    #load_stacje_to_db()
    #load_normy_to_db()
    #load_substancje_to_db()
    #load_substancja_regula_to_db()
    load_metody_pomiaru_to_db()

    # print(extract_data_from_GUGiK_API(lat=50.255072, lon=16.801641).text)
    # print(extract_data_from_GUGiK_API(lat=50.264611, lon=18.975028).text)
    # print(extract_data_from_GUGiK_API(lat=51.197886, lon=20.412946).text)
    # print(extract_data_from_GUGiK_API(lat=54.090278, lon=18.797222).text)


    # data = extract_data_from_GUGiK_API(50.682510, 16.617348)
    # parse_uldk_respone_to_terc(data)

def load_stanowiska_to_db():
    pass

def load_metody_pomiaru_to_db():

    metadata_sheets = load_excel_sheets(path_metadata)
    ark_stanowiska = metadata_sheets[list(metadata_sheets.keys())[1]]

    print(ark_stanowiska.columns.to_list())
    metoda_pomiaru = ark_stanowiska[['Czas uśredniania', 'Typ pomiaru']]

    metoda_pomiaru = metoda_pomiaru.rename(columns={
        'Czas uśredniania': 'czas_usredniania',
        'Typ pomiaru': 'typ_pomiaru'
    })

    print(metoda_pomiaru.head(20))

    metoda_pomiaru = metoda_pomiaru.dropna(subset=[
        'czas_usredniania',
        'typ_pomiaru'
    ])

    metoda_pomiaru = metoda_pomiaru.drop_duplicates(subset=[
        'czas_usredniania',
        'typ_pomiaru'
    ])

    metoda_pomiaru['opis_metody'] = None

    metoda_pomiaru = metoda_pomiaru.reset_index(drop=True)

    print(metoda_pomiaru.head(20))

    insert_records(
        table="metoda_pomiaru",
        records=metoda_pomiaru.to_dict(orient="records"),
        columns=['czas_usredniania', 'typ_pomiaru', 'opis_metody'],
        unique_columns=['czas_usredniania', 'typ_pomiaru']
    )

def load_substancja_regula_to_db():
    norms = pd.read_csv("Data/Raw/normy_powietrza_gios.csv", sep=";")
    norms = norms.replace({np.nan: None})

    metadata_sheets = load_excel_sheets(path_metadata)
    ark_stanowiska = metadata_sheets[list(metadata_sheets.keys())[1]]

    print(f"Kolumny: {ark_stanowiska.columns.to_list()}")

    substancje = ark_stanowiska[
        ["Wskaźnik - kod", "Wskaźnik"]
    ].copy()

    substancje = substancje.rename(columns={
        'Wskaźnik - kod': 'kod_wskaznika',
        'Wskaźnik': 'wskaznik',
    })

    print(f"Kolumny z substancja df: {substancje.columns.to_list()}")
    print(f"Wszystkich stanowisk jest: {len(substancje)}")
    
    substancje["kod_wskaznika"] = (
        substancje["kod_wskaznika"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
    )

    substancje["wskaznik"] = (
        substancje["wskaznik"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
    )

    substancje["jednostka_domyslna"] = None

    substancje = substancje.dropna(subset=[
        "kod_wskaznika",
    ])

    substancje = substancje.drop_duplicates(subset=[
        "kod_wskaznika",
    ])

    substancje = substancje.reset_index(drop=True)

    substancja_id_map = get_id_map(
        engine=get_engine(),
        table="substancja",
        key_column="kod_wskaznika",
        id_column="substancja_id",
        schema="gios"
    )

    norma_id_map = get_norma_map(
        engine=get_engine(),
        key_columns=[
            'typ_normy',
            'ochrona',
            'wartosc_normy',
            'czas_usredniania',
            'jednostka',
            'data_od'
        ]
    )

    substancje["substancja_id"] = substancje["kod_wskaznika"].map(substancja_id_map)
    
    norms["key_cols"] = list(zip(
        norms['typ_normy'],
        norms['ochrona'],
        norms['wartosc_normy'],
        norms['czas_usredniania'],
        norms['jednostka'],
        norms['data_od']
    ))
    norms["norma_id"] = norms["key_cols"].map(norma_id_map)

    print(f"norma cols: {norms.columns.to_list()}")
    print(f"substancja cols: {substancje.columns.to_list()}")

    print(substancje["kod_wskaznika"].to_list())

    substancja_regula = norms[['kod_substancji', 'norma_id']]

    substancje["kod_wskaznika"] = substancje["kod_wskaznika"].str.replace(r"\(PM10\)", "", regex=True)

    substancja_regula = (
    substancja_regula
        .merge(
            substancje[["kod_wskaznika", "substancja_id"]],
            left_on="kod_substancji",
            right_on="kod_wskaznika",
            how="left"
        )
        .drop(columns=["kod_wskaznika"])
    )

    substancja_regula["uwagi"] = None
    print(substancja_regula)
    substancja_regula = substancja_regula.drop(columns=["kod_substancji"])
    print(substancja_regula)

    insert_records(
        table="substancja_regula",
        records=substancja_regula.to_dict(orient="records"),
        columns=substancja_regula.columns.to_list(),
        unique_columns=["substancja_id", "norma_id"]
    )

#TODO uzupelnic domyslna jednostke jak wszystko bedzie zaladowane
def load_substancje_to_db():
    metadata_sheets = load_excel_sheets(path_metadata)
    ark_stanowiska = metadata_sheets[list(metadata_sheets.keys())[1]]

    print(f"Kolumny: {ark_stanowiska.columns.to_list()}")

    substancje = ark_stanowiska[
        ["Wskaźnik - kod", "Wskaźnik"]
    ].copy()

    substancje = substancje.rename(columns={
        'Wskaźnik - kod': 'kod_wskaznika',
        'Wskaźnik': 'wskaznik',
    })

    print(f"Kolumny z substancja df: {substancje.columns.to_list()}")
    print(f"Wszystkich stanowisk jest: {len(substancje)}")
    

    substancje["kod_wskaznika"] = (
        substancje["kod_wskaznika"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
    )

    substancje["wskaznik"] = (
        substancje["wskaznik"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
    )

    substancje["jednostka_domyslna"] = None

    substancje = substancje.dropna(subset=[
        "kod_wskaznika",
    ])

    substancje = substancje.drop_duplicates(subset=[
        "kod_wskaznika",
    ])

    substancje = substancje.reset_index(drop=True)

    print(substancje.head(80))

    cols = substancje.columns.to_list()
    insert_records(
        table="substancja",
        records=substancje.to_dict(orient="records"),
        columns=cols,
        unique_columns=["kod_wskaznika"]
    )

def load_normy_to_db():
    norms = pd.read_csv("Data/Raw/normy_powietrza_gios.csv", sep=";")
    norms = norms.replace({np.nan: None})

    print(norms.head(10))
    print(norms.columns)

    cols = [x for x in norms.columns if x not in ["kod_substancji", "nazwa_substancji"]]

    insert_records(
        table="norma",
        records=norms.to_dict(orient="records"),
        columns=cols,
        unique_columns=[
            "typ_normy",
            "ochrona",
            "wartosc_normy",
            "czas_usredniania",
            "jednostka",
            "data_od"
        ]
    )

def load_miejscowosci_to_db():
    engine = get_engine()

    simc_df = load_simc(path_simc)

    gmina_map = get_gmina_teryt_map(
        engine=engine,
        schema="gios",
    )

    result = prepare_miejscowosci_from_simc(
        simc_df=simc_df,
        gmina_map=gmina_map,
    )

    print(result.head())
    print(f"Liczba miejscowości do załadowania: {len(result)}")

    insert_records(
        table="miejscowosc",
        records=result.to_dict(orient="records"),
        columns=[
            "gmina_id",
            "nazwa_miejscowosci",
            "kod_simc",
        ],
        unique_columns=[
            "kod_simc",
        ],
    )

def load_stacje_to_db():
    engine = get_engine()

    metadata_sheets = load_excel_sheets(path_metadata)

    ark_stacje = metadata_sheets["STACJE"]

    # mapa (woj, miejsc): [id miejscowosci z danego woj]
    # miejscowosc_map["result"] -> mapa tylko tych dla ktorych (woj, miejsc) jest kluczem identyfikujacym
    # miejscowosc_map["ambigous"] -> mapa wszystkich dla ktorych klucz (woj, miejsc) zwraca liste a nie jeden element
    miejscowosc_map = get_miejscowosc_woj_name_map(
        engine=engine,
        schema="gios",
    )

    simc_map = get_miejscowosc_simc_map(
        engine=engine,
        schema="gios",
    )

    # prepared_stacje["all"] - zawiera wszystkie informacje robocze, stacje rozwiazane automatycznie, nierozwiazane, kolumny pomocniczne itp
    # prepared_stacje["cleaned"] - tylko wiersze z gotowym miejscowosc_id, kod_stacji, nazwa_stacji itp, gotowe do inserta
    # wazna rzecz!
    # prepare_stacje_from_metadata przygotowuje plik stacje_miejscowosci_problemy, gdzie wypisuje dlaczego automatyczne korekty
    # nie daly rady, podaje tez kilka kolumn pomocniczych do rozstrzygniecia problemu recznie, np wszystkie miejscowosci z danej gminy itp
    prepared_stacje = prepare_stacje_from_metadata(
        stacje_df=ark_stacje,
        miejscowosc_map=miejscowosc_map["result"],
        simc_map=simc_map,
        path_korekty=Path("Data") / "Corrections" / "Important" / "stacje_miejscowosci_problemy.csv",
    )

    # tutaj trzeba podac dataframe w postacji prepared_stacje (w zasadzie slownik z dataframe'ami)
    # apply_stacje_miejscowosci_corrections z kolei wczytuje plik z recznie przygotowanymi rozwiazaniami i aplikuje
    # zwraca stacje_cleaned_after_corrections.csv i stacje_errors_after_corrections.csv, jesli ten drugi plik ma jakiekolwiek rekordy
    # nalezy uzupelnic plik korekt stacje_miejscowosci_korekty
    corrected_stacje = apply_stacje_miejscowosci_corrections(
        dataframe_to_correct_manually=prepared_stacje,
        simc_map=simc_map,
        path_korekty=Path("Data") / "Corrections" / "Important" / "stacje_miejscowosci_korekty.csv",
        only_auto=True,
    )

    er_kor = pd.read_csv(
        Path("Data") / "Corrections" / "Important" / "stacje_miejscowosci_korekty.csv",
        sep=";",
        dtype=str,
    )

    auto_count = len(er_kor[er_kor["uzyc_automatycznie"] == "TAK"])
    non_auto_count = len(er_kor[er_kor["uzyc_automatycznie"] == "NIE"])

    print(f"Automatycznych: {auto_count}")
    print(f"Nieautomatycznych: {non_auto_count}")
    print(er_kor[er_kor["uzyc_automatycznie"] == "NIE"].head(20))

    stacje_do_bazy = corrected_stacje["cleaned"]

    insert_records(
        table="stacja",
        records=stacje_do_bazy.to_dict(orient="records"),
        columns=[
            "miejscowosc_id",
            "adres",
            "kod_stacji",
            "kod_miedzynarodowy",
            "nazwa_stacji",
            "stary_kod_stacji",
            "data_uruchomienia",
            "data_zamkniecia",
            "typ_stacji",
            "typ_obszaru",
            "rodzaj_stacji",
            "wgs84_fi_n",
            "wgs84_lambda_e",
            "uwagi",
        ],
        unique_columns=[
            "kod_stacji",
        ],
    )

def load_strefy_to_db():
    engine = get_engine()

    metadata_sheets = load_excel_sheets(path_metadata)

    ark_stanowiska = metadata_sheets[list(metadata_sheets.keys())[1]]

    columns_to_drop = list(ark_stanowiska.columns)
    columns_to_drop = [col for col in columns_to_drop if col not in ["Województwo", "Nazwa strefy", ]]
    ark_stanowiska = ark_stanowiska.drop(columns=columns_to_drop)

    # print(f"Arkusz {list(metadata_sheets.keys())[1]}:")
    # print(ark_stanowiska.head(20))
    # print(f"Liczba rekordow przed czyszczeniem: {ark_stanowiska.count(0)}")

    ark_stanowiska["nazwa_wojewodztwa"] = (
        ark_stanowiska["Województwo"]
        .astype("string")
        .str.strip()
    )
    ark_stanowiska["nazwa_wojewodztwa"] = ark_stanowiska["nazwa_wojewodztwa"].replace("", pd.NA)

    ark_stanowiska["nazwa_strefy"] = (
        ark_stanowiska["Nazwa strefy"]
        .astype("string")
        .str.strip()
    )
    ark_stanowiska["nazwa_strefy"] = ark_stanowiska["nazwa_strefy"].replace("", pd.NA)
    ark_stanowiska = ark_stanowiska.drop(columns=["Nazwa strefy", "Województwo"])
    ark_stanowiska = ark_stanowiska.dropna(subset=["nazwa_strefy", "nazwa_wojewodztwa"])
    ark_stanowiska = ark_stanowiska.drop_duplicates(subset=["nazwa_strefy", "nazwa_wojewodztwa"])
    ark_stanowiska.reset_index()

    ark_stanowiska = ark_stanowiska[
        ark_stanowiska["nazwa_wojewodztwa"].notna()
        & ark_stanowiska["nazwa_strefy"].notna()
    ]

    woj_name_to_id_map = get_id_map(
        engine=engine,
        table="wojewodztwo",
        key_column="nazwa_wojewodztwa",
        id_column="wojewodztwo_id",
        schema="gios"
    )

    result = pd.DataFrame()

    result["wojewodztwo_id"] = ark_stanowiska["nazwa_wojewodztwa"].map(woj_name_to_id_map)
    result["nazwa_wojewodztwa"] = ark_stanowiska["nazwa_wojewodztwa"]
    result["nazwa_strefy"] = ark_stanowiska["nazwa_strefy"]
    result["typ_strefy"] = ark_stanowiska["nazwa_strefy"].str.split().str[0]
    result = result.drop(columns=["nazwa_wojewodztwa"])
    result.reset_index(inplace=True, drop=True)

    print(result.head())

    insert_records(
        table="strefa",
        records=result.to_dict(orient="records"),
        columns=[
            "wojewodztwo_id",
            "nazwa_strefy",
            "typ_strefy",
        ],
        unique_columns=[
            "wojewodztwo_id",
            "nazwa_strefy",
            "typ_strefy"
        ],
    )

def load_terc_to_db():
    engine = get_engine()

    terc_df = load_terc(PATH_TERC)

    woj_df = prepare_wojewodztwa_from_terc(terc_df)

    insert_records(
        table="wojewodztwo",
        records=woj_df.to_dict(orient="records"),
        columns=["kod_wojewodztwa", "nazwa_wojewodztwa"],
        unique_columns=["nazwa_wojewodztwa"],
    )

    woj_map = get_id_map(
        engine=engine,
        table="wojewodztwo",
        key_column="kod_wojewodztwa",
        id_column="wojewodztwo_id",
    )

    powiat_df = prepare_powiaty_from_terc(
        terc_df=terc_df,
        woj_map=woj_map,
    )

    insert_records(
        table="powiat",
        records=powiat_df.to_dict(orient="records"),
        columns=[
            "wojewodztwo_id",
            "kod_powiatu",
            "nazwa_powiatu",
            "typ_powiatu",
        ],
        unique_columns=[
            "wojewodztwo_id",
            "kod_powiatu",
        ],
    )

    powiat_map = get_powiat_terc_map(engine)

    stoszowice_powiat_id = powiat_map[("02", "24")]

    print("powiat_id dla 02/24:", stoszowice_powiat_id)

    gmina_df = prepare_gminy_from_terc(
        terc_df=terc_df,
        powiat_map=powiat_map,
    )

    print("DEBUG powiat_map:")
    print("02/01:", powiat_map.get(("02", "01")))
    print("02/02:", powiat_map.get(("02", "02")))
    print("02/03:", powiat_map.get(("02", "03")))
    print("02/24:", powiat_map.get(("02", "24")))
    print("Liczba powiatów w powiat_map:", len(powiat_map))

    print("DEBUG: PATH_TERC =", PATH_TERC)

    print("DEBUG: Stoszowice bezpośrednio w terc_df:")
    print(
        terc_df[
            (terc_df["WOJ"] == "02")
            & (terc_df["POW"] == "24")
            & (terc_df["GMI"] == "04")
            & (terc_df["RODZ"] == "2")
        ]
    )

    print("DEBUG: powiat_map dla 02/24:")
    print(powiat_map.get(("02", "24")))

    print("DEBUG: Stoszowice w gmina_df po nazwie:")
    print(
        gmina_df[
            gmina_df["nazwa_gminy"].astype("string").str.contains(
                "Stoszowice",
                case=False,
                na=False,
            )
        ]
    )

    print("DEBUG: duplikaty po kluczu unique w gmina_df:")
    dupes = gmina_df[
        gmina_df.duplicated(
            subset=["powiat_id", "kod_gminy", "rodzaj_gminy"],
            keep=False,
        )
    ]
    print("Liczba duplikatów:", len(dupes))
    print(dupes.head(50))

    print("DEBUG: liczba unikalnych kluczy gmin:")
    print(
        gmina_df[
            ["powiat_id", "kod_gminy", "rodzaj_gminy"]
        ].drop_duplicates().shape[0]
    )

    insert_records(
        table="gmina",
        records=gmina_df.to_dict(orient="records"),
        columns=[
            "powiat_id",
            "kod_gminy",
            "rodzaj_gminy",
            "nazwa_gminy",
            "typ_gminy",
        ],
        unique_columns=[
            "powiat_id",
            "kod_gminy",
            "rodzaj_gminy",
        ],
    )

    print("TERC załadowany.")