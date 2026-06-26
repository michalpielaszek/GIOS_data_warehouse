from Scripts.Database.connection import get_engine
from sqlalchemy import text
import pandas as pd

from typing import Any
import re

from sqlalchemy import text
from sqlalchemy.engine import Engine

from Scripts.Database.connection import get_engine

from datetime import datetime, date


def validate_identifier(name: str) -> None:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
        raise ValueError(f"Niepoprawna nazwa SQL: {name}")

def get_strefa_wojewodztwo_map(
    engine: Engine,
    schema: str = "gios",
    key_columns: list[str] | None = ['nazwa_strefy', 'nazwa_wojewodztwa']
) -> dict[Any, int]:
    
    cols = ["nazwa_strefy", "nazwa_wojewodztwa"]

    if key_columns is not None:
        cols = key_columns

    cols_sql = ", ".join(cols)

    where_sql = " AND ".join(
        f"{col} IS NOT NULL"
        for col in cols
    )

    sql = text(f"""
        SELECT {cols_sql}, strefa_id
        FROM {schema}.strefa
        JOIN {schema}.wojewodztwo 
            ON {schema}.strefa.wojewodztwo_id = {schema}.wojewodztwo.wojewodztwo_id
        WHERE {where_sql}
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    return {
        (row[0], row[1]): row[2]
        for row in rows
    }

def get_metoda_pomiaru_map(
    engine: Engine,
    key_columns: list[str] | None = ['czas_usredniania', 'typ_pomiaru']
)->dict[Any, int]:
    
    cols = 'czas_usredniania', 'typ_pomiaru'
    if key_columns != None:
        cols = ""
        for key_col in key_columns:
            cols += key_col + ","

    sql = text(f"""
        SELECT {cols} metoda_id
        FROM gios.metoda_pomiaru
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    return {
        (row[0], row[1]): row[2]
        for row in rows
    }
      
def get_table_columns(
    engine: Engine,
    table: str,
    schema: str = "gios"
) -> set[str]:
    sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table;
    """)

    with engine.connect() as conn:
        result = conn.execute(sql, {"schema": schema, "table": table})
        return {row[0] for row in result}

def get_stacja_map(
    engine: Engine,
    key_column: str = "kod_stacji"
) -> dict[Any, int]:
    sql = text(f"""
        SELECT {key_column}, stacja_id
        FROM gios.stacja
        WHERE {key_column} IS NOT NULL
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    result = {}

    for row in rows:
        key = str(row[0]).strip()
        stacja_id = int(row[1])

        result[key] = stacja_id

    return result

def insert_records(
    table: str,
    records: list[dict[str, Any]],
    columns: list[str] | None = None,
    unique_columns: list[str] | None = None,
    schema: str = "gios",
) -> None:
    if not records:
        print("Brak rekordów do wstawienia.")
        return

    engine = get_engine()

    validate_identifier(schema)
    validate_identifier(table)

    if columns is None:
        columns = list(records[0].keys())

    for col in columns:
        validate_identifier(col)

    if unique_columns is not None:
        for col in unique_columns:
            validate_identifier(col)

    existing_columns = get_table_columns(engine, table=table, schema=schema)

    missing_columns = set(columns) - existing_columns
    if missing_columns:
        raise ValueError(
            f"Tabela {schema}.{table} nie ma kolumn: {missing_columns}"
        )

    if unique_columns:
        missing_unique_columns = set(unique_columns) - existing_columns
        if missing_unique_columns:
            raise ValueError(
                f"Tabela {schema}.{table} nie ma kolumn UNIQUE: {missing_unique_columns}"
            )

    for i, record in enumerate(records):
        missing_keys = set(columns) - set(record.keys())
        if missing_keys:
            raise ValueError(
                f"Rekord nr {i} nie ma kluczy: {missing_keys}"
            )

    columns_sql = ", ".join(columns)
    values_sql = ", ".join(f":{col}" for col in columns)

    if unique_columns:
        unique_sql = ", ".join(unique_columns)
        on_conflict_sql = f"ON CONFLICT ({unique_sql}) DO NOTHING"
    else:
        on_conflict_sql = ""

    sql = text(f"""
        INSERT INTO {schema}.{table} (
            {columns_sql}
        )
        VALUES (
            {values_sql}
        )
        {on_conflict_sql};
    """)

    clean_records = [
        {col: record[col] for col in columns}
        for record in records
    ]

    with engine.begin() as conn:
        conn.execute(sql, clean_records)

    print(f"Przetworzono rekordy dla {schema}.{table}: {len(clean_records)}")

def get_substancja_map(
    engine: Engine,
    key_column: str = "kod_wskaznika"
) -> dict[Any, int]:
    sql = text(f"""
        SELECT {key_column}, substancja_id
        FROM gios.substancja
        WHERE {key_column} IS NOT NULL
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    return {
        row[0]: row[1]
        for row in rows
    }



# (typ_normy, ochrona, wartosc_normy, czas_usredniania, jednostka, data_od) -> norma_id
def get_norma_map(
        engine: Engine,
        key_columns: list[str] | None = ['typ_normy', 'ochrona', 'wartosc_normy', 'czas_usredniania', 'jednostka', 'data_od']
    )->dict[Any, int]:

    cols = "typ_normy, ochrona, wartosc_normy, czas_usredniania, jednostka, data_od,"
    if key_columns != None:
        cols = ""
        for key_col in key_columns:
            cols += key_col + ","

    sql = text(f"""
        SELECT {cols} norma_id
        FROM gios.norma
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    result = {}

    for row in rows:
        data_od = row[5]

        if data_od is not None:
            data_od = data_od.strftime(f"%Y-%m-%d")

        key = (
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            data_od,
        )

        norma_id = row[6]

        result[key] = norma_id

    return result

def get_id_map(
    engine: Engine,
    table: str,
    key_column: str,
    id_column: str,
    schema: str = "gios",
) -> dict[Any, int]:
    sql = text(f"""
        SELECT {key_column}, {id_column}
        FROM {schema}.{table};
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    return {row[0]: row[1] for row in rows}

def get_powiat_terc_map(engine: Engine, schema: str = "gios") -> dict[tuple[str, str], int]:
    sql = text(f"""
        SELECT
            w.kod_wojewodztwa,
            p.kod_powiatu,
            p.powiat_id
        FROM {schema}.powiat p
        JOIN {schema}.wojewodztwo w
            ON p.wojewodztwo_id = w.wojewodztwo_id;
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    return {
        (row[0], row[1]): row[2]
        for row in rows
    }

def get_gmina_teryt_map(engine: Engine, schema: str = "gios") -> dict[tuple[str, str, str, str], int]:
    sql = text(f"""
        SELECT
            w.kod_wojewodztwa,
            p.kod_powiatu,
            g.kod_gminy,
            g.rodzaj_gminy,
            g.gmina_id
        FROM {schema}.gmina g
        JOIN {schema}.powiat p
            ON g.powiat_id = p.powiat_id
        JOIN {schema}.wojewodztwo w
            ON p.wojewodztwo_id = w.wojewodztwo_id;
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    return {
        (row[0], row[1], row[2], row[3]): row[4]
        for row in rows
    }

def get_woj_pow_gm_rg_nazwa_to_miejscowosc_id_map(
    engine: Engine,
    schema: str = "gios",
) -> dict[tuple[str, str, str, str, str], int]:
    """
    Tworzy mapę:

        (WOJ, POW, GMI, RODZ_GMI, NAZWA_MIEJSCOWOSCI) -> miejscowosc_id

    Przykład klucza:

        ("02", "24", "04", "2", "STOSZOWICE")

    Przykład wartości:

        1234
    """

    sql = text(f"""
        SELECT
            w.kod_wojewodztwa,
            p.kod_powiatu,
            g.kod_gminy,
            g.rodzaj_gminy,
            UPPER(m.nazwa_miejscowosci) AS nazwa_miejscowosci,
            m.miejscowosc_id
        FROM {schema}.miejscowosc m
        JOIN {schema}.gmina g
            ON m.gmina_id = g.gmina_id
        JOIN {schema}.powiat p
            ON g.powiat_id = p.powiat_id
        JOIN {schema}.wojewodztwo w
            ON p.wojewodztwo_id = w.wojewodztwo_id
        WHERE m.kod_simc IS NOT NULL;
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    result: dict[tuple[str, str, str, str, str], int] = {}

    for row in rows:
        woj = str(row[0]).strip()
        powiat = str(row[1]).strip()
        gmina = str(row[2]).strip()
        rodzaj_gminy = str(row[3]).strip()
        nazwa_miejscowosci = str(row[4]).strip()
        miejscowosc_id = row[5]

        key = (
            woj,
            powiat,
            gmina,
            rodzaj_gminy,
            nazwa_miejscowosci,
        )

        result[key] = miejscowosc_id

    return result

# szuka id po tym samym kluczu tylko bez rodzaju gminy, w bazie gios byly problemy
# czasami podawane byly miejscowosci ktore mialy dwuczlonowe nazwy co moglo byc uproszczeniem ze dana stacja
# lezy na pograniczu dwoch miejscowosci
# ponadto gugik zwracal czasami dla danych koordynatow bledny rodzaj gminy (albo ja go nie rozumiem)

# Przykład:
#   04 | 14 | 09 | 03 - teryt zwracany przez gugik
#   Świecie Przechowo - nazwa miejscowosci w gios
#   oficjalna baza GUS, rozdziela to na dwa kodu SIMC (dwie różne miejscowości) na wyższym poziomie agregacji tj. z innym rodzajem gminy
#   z TERYT GUS mamy dwa rekordy:
#       - 04 | 14 | 09 | 4 , SIMC=0929687, Przechowo
#       - 04 | 14 | 09 | 4 , SIMC=0929664, Świecie
#   czyli prawie to samo ale rodzaj jest inny i dzieli się na dwie miejscowosci

# stąd motywacja żeby szukać miejscowości na wyższym poziomie agregacji administracyjnej - bez rodzaju gminy
# a przynajmniej tam gdzie nie wystepuje zaden przypadek albo jest inny konflikt
def get_woj_pow_gm_nazwa_to_miejscowosc_id_map(
    engine: Engine,
    schema: str = "gios",
) -> dict[str, dict]:
    """
    Tworzy mapy dla klucza:

        (WOJ, POW, GMI, NAZWA_MIEJSCOWOSCI)

    Zwraca:

        {
            "resolved_map": {
                (WOJ, POW, GMI, NAZWA): miejscowosc_id
            },

            "ambiguous_map": {
                (WOJ, POW, GMI, NAZWA): [miejscowosc_id_1, miejscowosc_id_2, ...]
            }
        }

    resolved_map zawiera tylko jednoznaczne przypadki.
    ambiguous_map zawiera przypadki, których nie wolno rozwiązać automatycznie.
    """

    sql = text(f"""
        SELECT
            w.kod_wojewodztwa,
            p.kod_powiatu,
            g.kod_gminy,
            UPPER(m.nazwa_miejscowosci) AS nazwa_miejscowosci,
            m.miejscowosc_id
        FROM {schema}.miejscowosc m
        JOIN {schema}.gmina g
            ON m.gmina_id = g.gmina_id
        JOIN {schema}.powiat p
            ON g.powiat_id = p.powiat_id
        JOIN {schema}.wojewodztwo w
            ON p.wojewodztwo_id = w.wojewodztwo_id
        WHERE m.kod_simc IS NOT NULL;
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    grouped_ids: dict[tuple[str, str, str, str], list[int]] = {}

    for row in rows:
        woj = str(row[0]).strip()
        powiat = str(row[1]).strip()
        gmina = str(row[2]).strip()
        nazwa_miejscowosci = str(row[3]).strip()
        miejscowosc_id = row[4]

        key = (
            woj,
            powiat,
            gmina,
            nazwa_miejscowosci,
        )

        if key not in grouped_ids:
            grouped_ids[key] = []

        grouped_ids[key].append(miejscowosc_id)

    resolved_map: dict[tuple[str, str, str, str], int] = {}
    ambiguous_map: dict[tuple[str, str, str, str], list[int]] = {}

    for key in grouped_ids:
        ids = grouped_ids[key]

        unique_ids = []

        for miejscowosc_id in ids:
            if miejscowosc_id not in unique_ids:
                unique_ids.append(miejscowosc_id)

        if len(unique_ids) == 1:
            resolved_map[key] = unique_ids[0]
        else:
            ambiguous_map[key] = unique_ids

    #print(f"Mapa bez rodzaju_gminy - jednoznaczne klucze: {len(resolved_map)}")
    #print(f"Mapa bez rodzaju_gminy - niejednoznaczne klucze: {len(ambiguous_map)}")

    return {
        "resolved_map": resolved_map,
        "ambiguous_map": ambiguous_map,
    }

def get_miejscowosc_simc_map(
    engine: Engine,
    schema: str = "gios",
) -> dict[str, int]:
    
    '''
    kod_simc -> miejscowosc_id
    '''
    sql = text(f"""
        SELECT kod_simc, miejscowosc_id
        FROM {schema}.miejscowosc
        WHERE kod_simc IS NOT NULL;
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    return {
        str(row[0]).strip(): row[1] for row in rows
    }

def get_miejscowosc_woj_name_map(
    engine: Engine,
    schema: str = "gios",
) -> dict[tuple[str, str], int]:
    """
    Tworzy mapę:
        (nazwa_wojewodztwa, nazwa_miejscowosci) -> miejscowosc_id

    Do wyniku trafiają tylko jednoznaczne pary, czyli takie,
    dla których w bazie istnieje dokładnie jedno miejscowosc_id.

    Przykład wyniku:
        {
            ("DOLNOŚLĄSKIE", "WROCŁAW"): 123,
            ("MAZOWIECKIE", "WARSZAWA"): 456,
        }

    Jeśli np. w jednym województwie istnieje kilka miejscowości
    o tej samej nazwie, taka para jest pomijana.
    """

    sql = text(f"""
        SELECT
            UPPER(w.nazwa_wojewodztwa) AS nazwa_wojewodztwa,
            UPPER(m.nazwa_miejscowosci) AS nazwa_miejscowosci,
            m.miejscowosc_id
        FROM {schema}.miejscowosc m
        JOIN {schema}.gmina g
            ON m.gmina_id = g.gmina_id
        JOIN {schema}.powiat p
            ON g.powiat_id = p.powiat_id
        JOIN {schema}.wojewodztwo w
            ON p.wojewodztwo_id = w.wojewodztwo_id;
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    # Krok 1:
    # Grupujemy miejscowosc_id po parze:
    # (województwo, miejscowość)
    #
    # Przykład:
    # {
    #     ("DOLNOŚLĄSKIE", "WROCŁAW"): [123],
    #     ("DOLNOŚLĄSKIE", "RÓŻANA"): [12, 98, 341],
    # }
    grouped_ids: dict[tuple[str, str], list[int]] = {}

    for row in rows:
        wojewodztwo_name = str(row[0]).strip()
        miejscowosc_name = str(row[1]).strip()
        miejscowosc_id = row[2]

        key = (wojewodztwo_name, miejscowosc_name)

        if key not in grouped_ids:
            grouped_ids[key] = []

        grouped_ids[key].append(miejscowosc_id)

    # Krok 2:
    # Budujemy finalną mapę tylko z par jednoznacznych.
    #
    # Jeśli lista ID ma długość 1, to wiemy dokładnie,
    # które miejscowosc_id pasuje do tej pary.
    result: dict[tuple[str, str], int] = {}

    # Osobno zbieramy niejednoznaczne przypadki do debugowania.
    ambiguous: dict[tuple[str, str], list[int]] = {}

    for key in grouped_ids:

        miejscowosc_ids = grouped_ids[key]

        if len(miejscowosc_ids) == 1:
            result[key] = miejscowosc_ids[0]
        else:
            ambiguous[key] = miejscowosc_ids

    # Krok 3:
    # Informacyjnie wypisujemy przykłady par, których nie da się
    # bezpiecznie zmapować po samej nazwie województwa i miejscowości.
    if ambiguous:
        print("Uwaga: są niejednoznaczne miejscowości w ramach województwa.")
        print("Takie pary nie trafią do mapy automatycznej.")
        print("Przykłady:")

        counter = 0

        for key in ambiguous:
            print(key, ambiguous[key])

            counter += 1

            if counter >= 20:
                break

    print(type(ambiguous))
    return {
        "result": result,
        "ambigous": ambiguous
    }

def get_or_create_czas_id(
    engine: Engine,
    data_czas: datetime | date | str,
    schema: str = "gios",
) -> int:
    if isinstance(data_czas, str):
        data_czas = pd.to_datetime(data_czas)

    if isinstance(data_czas, date) and not isinstance(data_czas, datetime):
        data_czas = datetime.combine(data_czas, datetime.min.time())

    sql_insert = text(f"""
        INSERT INTO {schema}.czas (
            data_czas,
            data,
            rok,
            miesiac,
            dzien_miesiaca,
            godzina
        )
        VALUES (
            :data_czas,
            :data,
            :rok,
            :miesiac,
            :dzien_miesiaca,
            :godzina
        )
        ON CONFLICT (data_czas) DO NOTHING;
    """)

    sql_select = text(f"""
        SELECT czas_id
        FROM {schema}.czas
        WHERE data_czas = :data_czas;
    """)

    params = {
        "data_czas": data_czas,
        "data": data_czas.date(),
        "rok": data_czas.year,
        "miesiac": data_czas.month,
        "dzien_miesiaca": data_czas.day,
        "godzina": data_czas.hour,
    }

    with engine.begin() as conn:
        conn.execute(sql_insert, params)

        result = conn.execute(
            sql_select,
            {"data_czas": data_czas},
        ).fetchone()

    if result is None:
        raise ValueError(f"Nie udało się znaleźć ani utworzyć czasu: {data_czas}")

    # to jest po prostu id
    return result[0]

def get_miejscowosci_candidates_for_gmina(
    engine: Engine,
    woj: str,
    powiat: str,
    gmina: str,
    rodzaj_gminy: str,
    schema: str = "gios",
) -> str:
    """
    Zwraca wszystkie miejscowości z konkretnej gminy:
    NAZWA (kod_simc) | NAZWA2 (kod_simc2)
    """

    sql = text(f"""
        SELECT
            m.nazwa_miejscowosci,
            m.kod_simc
        FROM {schema}.miejscowosc m
        JOIN {schema}.gmina g
            ON m.gmina_id = g.gmina_id
        JOIN {schema}.powiat p
            ON g.powiat_id = p.powiat_id
        JOIN {schema}.wojewodztwo w
            ON p.wojewodztwo_id = w.wojewodztwo_id
        WHERE w.kod_wojewodztwa = :woj
          AND p.kod_powiatu = :powiat
          AND g.kod_gminy = :gmina
          AND g.rodzaj_gminy = :rodzaj_gminy
        ORDER BY m.nazwa_miejscowosci, m.kod_simc;
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, {
            "woj": woj,
            "powiat": powiat,
            "gmina": gmina,
            "rodzaj_gminy": rodzaj_gminy,
        }).fetchall()

    candidates = []

    for row in rows:
        nazwa = str(row[0]).strip()
        kod_simc = str(row[1]).strip()
        candidates.append(f"{nazwa} ({kod_simc})")

    return " | ".join(candidates)

def build_name_like_pattern(name: str) -> str:
    """
    Z nazwy miejscowości buduje wzorzec do ILIKE.

    Przykład:
        "NOWA RUDA - SŁUPIEC" -> "%NOWA%RUDA%SŁUPIEC%"
        "STARGARD SZCZECIŃSKI" -> "%STARGARD%SZCZECIŃSKI%"
    """

    if name is None:
        return "%"

    normalized_name = str(name).strip().upper()

    words = re.findall(r"[A-ZĄĆĘŁŃÓŚŹŻ0-9]+", normalized_name)

    if not words:
        return "%"

    pattern = "%"

    for word in words:
        pattern = pattern + word + "%"

    return pattern

def get_miejscowosci_candidates_for_woj_and_name(
    engine: Engine,
    wojewodztwo_nazwa: str,
    miejscowosc_nazwa: str,
    schema: str = "gios",
) -> str:
    """
    Dla stacji bez współrzędnych:
    szuka miejscowości w danym województwie podobnych do nazwy z GIOŚ.

    Szukanie:
        województwo musi się zgadzać dokładnie,
        nazwa miejscowości jest szukana przez ILIKE pattern:
        %CZLON1%CZLON2%CZLON3%
    """

    name_pattern = build_name_like_pattern(miejscowosc_nazwa)

    sql = text(f"""
        SELECT
            w.nazwa_wojewodztwa,
            p.nazwa_powiatu,
            g.nazwa_gminy,
            g.typ_gminy,
            m.nazwa_miejscowosci,
            m.kod_simc
        FROM {schema}.miejscowosc m
        JOIN {schema}.gmina g
            ON m.gmina_id = g.gmina_id
        JOIN {schema}.powiat p
            ON g.powiat_id = p.powiat_id
        JOIN {schema}.wojewodztwo w
            ON p.wojewodztwo_id = w.wojewodztwo_id
        WHERE UPPER(w.nazwa_wojewodztwa) = :wojewodztwo_nazwa
          AND UPPER(m.nazwa_miejscowosci) ILIKE :name_pattern
        ORDER BY p.nazwa_powiatu, g.nazwa_gminy, m.nazwa_miejscowosci;
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, {
            "wojewodztwo_nazwa": str(wojewodztwo_nazwa).strip().upper(),
            "name_pattern": name_pattern,
        }).fetchall()

    candidates = []

    for row in rows:
        woj = str(row[0]).strip()
        powiat = str(row[1]).strip()
        gmina = str(row[2]).strip()
        typ_gminy = str(row[3]).strip()
        miejscowosc = str(row[4]).strip()
        kod_simc = str(row[5]).strip()

        candidate = (
            f"{miejscowosc} ({kod_simc})"
            f" | gmina: {gmina}"
            f" | typ: {typ_gminy}"
            f" | powiat: {powiat}"
            f" | woj: {woj}"
        )

        candidates.append(candidate)

    return " | ".join(candidates)

# def insert_column(table: str, records: list[dict[str: any]], keys: list) -> None:
#     '''
#     dict[keys] = values
#     keys - kolumny
#     values - wartosci w kolumnach
#     records - jakie rekordy należy dodać

#     dla wielu kolumn:
#     sql = text("""
#         INSERT INTO gios.stacja (
#             kod_stacji,
#             nazwa_stacji,
#             adres
#         )
#         VALUES (
#             :kod_stacji,
#             :nazwa_stacji,
#             :adres
#         )
#     """)

#     wtedy przykladowe records:
#     records = [
#         {
#             "kod_stacji": "DsBialka",
#             "nazwa_stacji": "Białka",
#             "adres": "Białka 1"
#         },
#         {
#             "kod_stacji": "MzWarsz",
#             "nazwa_stacji": "Warszawa",
#             "adres": None
#         }
#     ]
#     '''
#     engine = get_engine()

#     sql = text(f"""
#         INSERT INTO {table} (
#             {keys}
#         )
#         VALUES (
#             :{keys}
#         )
#         ON CONFLICT ({keys}) DO NOTHING;
#     """)

#     with engine.begin() as conn:
#         conn.execute(sql, records)

#     print(f"Przetworzono województwa: {len(records)}")