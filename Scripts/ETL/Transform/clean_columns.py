import pandas as pd
import numpy as np
import time
from pathlib import Path 
from Scripts.Database.connection import get_engine

from Scripts.ETL.Extract.data_exctract import (
    transform_epsg4326_to_epsg2180,
    parse_uldk_response_to_terc_and_save,
    extract_data_from_GUGiK_API
)

from Scripts.ETL.Load.data_load import (
    get_woj_pow_gm_rg_nazwa_to_miejscowosc_id_map,
    get_miejscowosci_candidates_for_gmina,
    get_miejscowosci_candidates_for_woj_and_name,
    get_woj_pow_gm_nazwa_to_miejscowosc_id_map
)

'''
def prepare_stacje_from_metadata(
    stacje_df: pd.DataFrame,
    miejscowosc_map: dict[tuple[str, str], int],
    simc_map: dict[str, int],
    path_korekty: Path,
) -> pd.DataFrame:
    """
    Przygotowuje dane z arkusza STACJE do inserta do tabeli gios.stacja.

    Kolejność ustalania miejscowosc_id:
    1. Próba po (wojewodztwo, miejscowosc).
    2. Jeśli brak — próba przez API GUGiK po współrzędnych.
    3. Jeśli nadal brak — zapis do pliku korekt.
    """

    # Na razie nie używamy simc_map w tej wersji.
    # Zostawiamy parametr, żeby nie rozwalać wywołania funkcji.
    _ = simc_map

    stacje_df = stacje_df.copy()
    stacje_df = stacje_df.reset_index(drop=True)

    required_columns = [
        "Kod stacji",
        "Kod międzynarodowy",
        "Nazwa stacji",
        "Stary Kod stacji \n(o ile inny od aktualnego)",
        "Data uruchomienia",
        "Data zamknięcia",
        "Typ stacji",
        "Typ obszaru",
        "Rodzaj stacji",
        "Województwo",
        "Miejscowość",
        "Adres",
        "WGS84 φ N",
        "WGS84 λ E",
    ]

    missing_columns = set(required_columns) - set(stacje_df.columns)

    if missing_columns:
        raise ValueError(
            f"Arkusz STACJE nie ma wymaganych kolumn: {missing_columns}. "
            f"Dostępne kolumny: {list(stacje_df.columns)}"
        )

    result = pd.DataFrame()

    woj = (
        stacje_df["Województwo"]
        .astype("string")
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
    )

    miejscowosc = (
        stacje_df["Miejscowość"]
        .astype("string")
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
    )

    result["wojewodztwo_miejscowosc"] = list(zip(woj, miejscowosc))
    result["miejscowosc_id"] = result["wojewodztwo_miejscowosc"].map(miejscowosc_map)

    result["kod_stacji"] = (
        stacje_df["Kod stacji"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["kod_miedzynarodowy"] = (
        stacje_df["Kod międzynarodowy"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["nazwa_stacji"] = (
        stacje_df["Nazwa stacji"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["stary_kod_stacji"] = (
        stacje_df["Stary Kod stacji \n(o ile inny od aktualnego)"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["data_uruchomienia"] = pd.to_datetime(
        stacje_df["Data uruchomienia"],
        errors="coerce",
    ).dt.date.to_numpy()

    result["data_zamkniecia"] = pd.to_datetime(
        stacje_df["Data zamknięcia"],
        errors="coerce",
    ).dt.date.to_numpy()

    result["typ_stacji"] = (
        stacje_df["Typ stacji"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["typ_obszaru"] = (
        stacje_df["Typ obszaru"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["rodzaj_stacji"] = (
        stacje_df["Rodzaj stacji"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["adres"] = (
        stacje_df["Adres"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["wgs84_fi_n"] = pd.to_numeric(
        stacje_df["WGS84 φ N"],
        errors="coerce",
    ).to_numpy()

    result["wgs84_lambda_e"] = pd.to_numeric(
        stacje_df["WGS84 λ E"],
        errors="coerce",
    ).to_numpy()

    result["wojewodztwo_nazwa"] = woj.to_numpy()
    result["miejscowosc_nazwa"] = miejscowosc.to_numpy()

    # Mapa:
    # (WOJ, POW, GMI, RODZ_GMINY, NAZWA_MIEJSCOWOSCI) -> miejscowosc_id
    miejscowosc_full_map = get_woj_pow_gm_rg_nazwa_to_miejscowosc_id_map(
        engine=get_engine(),
        schema="gios",
    )

    cache_path = Path("Data") / "Temp" / "gugik_stacje_cache.csv"

    api_success_count = 0
    api_failed_count = 0
    failed_rows = []

    # Przechodzimy tylko po tych stacjach, które nie dostały miejscowosc_id
    # z pierwszego prostego mapowania.
    missing_indexes = result[result["miejscowosc_id"].isna()].index

    for index in missing_indexes:
        row = result.loc[index]

        lat = row["wgs84_fi_n"]
        lon = row["wgs84_lambda_e"]

        # -999 oznacza brak sensownych współrzędnych.
        # Nie pytamy API, bo to nie jest realny punkt na mapie.
        if pd.isna(lat) or pd.isna(lon) or lat == -999 or lon == -999:
            api_failed_count += 1

            failed_rows.append({
                "kod_stacji": row["kod_stacji"],
                "nazwa_stacji": row["nazwa_stacji"],
                "wojewodztwo_nazwa": row["wojewodztwo_nazwa"],
                "miejscowosc_nazwa": row["miejscowosc_nazwa"],
                "adres": row["adres"],
                "wgs84_fi_n": row["wgs84_fi_n"],
                "wgs84_lambda_e": row["wgs84_lambda_e"],
                "powod": "brak_poprawnych_wspolrzednych",
            })

            continue

        time.sleep(0.1)

        response = extract_data_from_GUGiK_API(
            lat=lat,
            lon=lon,
        )

        if response is None:
            api_failed_count += 1

            failed_rows.append({
                "kod_stacji": row["kod_stacji"],
                "nazwa_stacji": row["nazwa_stacji"],
                "wojewodztwo_nazwa": row["wojewodztwo_nazwa"],
                "miejscowosc_nazwa": row["miejscowosc_nazwa"],
                "adres": row["adres"],
                "wgs84_fi_n": row["wgs84_fi_n"],
                "wgs84_lambda_e": row["wgs84_lambda_e"],
                "powod": "brak_odpowiedzi_api",
            })

            continue

        parsed = parse_uldk_response_to_terc_and_save(
            response=response,
            kod_stacji=row["kod_stacji"],
            wgs84_fi_n=row["wgs84_fi_n"],
            wgs84_lambda_e=row["wgs84_lambda_e"],
            cache_path=cache_path,
        )

        if parsed is None:
            api_failed_count += 1

            failed_rows.append({
                "kod_stacji": row["kod_stacji"],
                "nazwa_stacji": row["nazwa_stacji"],
                "wojewodztwo_nazwa": row["wojewodztwo_nazwa"],
                "miejscowosc_nazwa": row["miejscowosc_nazwa"],
                "adres": row["adres"],
                "wgs84_fi_n": row["wgs84_fi_n"],
                "wgs84_lambda_e": row["wgs84_lambda_e"],
                "powod": "api_nie_zwrocilo_teryt",
            })

            continue

        woj_kod, powiat_kod, gmina_kod, rodzaj_gminy = parsed

        nazwa_miejscowosci = str(row["miejscowosc_nazwa"]).strip().upper()

        key = (
            woj_kod,
            powiat_kod,
            gmina_kod,
            rodzaj_gminy,
            nazwa_miejscowosci,
        )

        miejscowosc_id = miejscowosc_full_map.get(key)

        if miejscowosc_id is None:
            api_failed_count += 1

            failed_rows.append({
                "kod_stacji": row["kod_stacji"],
                "nazwa_stacji": row["nazwa_stacji"],
                "wojewodztwo_nazwa": row["wojewodztwo_nazwa"],
                "miejscowosc_nazwa": row["miejscowosc_nazwa"],
                "adres": row["adres"],
                "wgs84_fi_n": row["wgs84_fi_n"],
                "wgs84_lambda_e": row["wgs84_lambda_e"],
                "powod": f"brak_miejscowosc_id_dla_klucza_{key}",
            })

            continue

        result.at[index, "miejscowosc_id"] = miejscowosc_id
        api_success_count += 1

    print(f"Za pomocą API udało się uzupełnić miejscowosc_id dla: {api_success_count}")
    print(f"Nie udało się uzupełnić miejscowosc_id dla: {api_failed_count}")

    if failed_rows:
        failed_df = pd.DataFrame(failed_rows)

        path_korekty.parent.mkdir(parents=True, exist_ok=True)

        failed_df.to_csv(
            path_korekty,
            sep=";",
            index=False,
            encoding="utf-8-sig",
        )

        print(f"Zapisano stacje do ręcznej korekty: {path_korekty}")
        print(failed_df)

    # Do inserta puszczamy tylko rekordy z miejscowosc_id.
    # Te bez miejscowosc_id zostają w pliku korekt.

    unsolved = result[result["miejscowosc_id"].isna()].copy()

    unsolved.to_csv(
        path_korekty,
        sep=";",
        index=False,
        encoding="utf-8-sig",
    )

    result = result.dropna(subset=[
        "miejscowosc_id",
        "kod_stacji",
        "nazwa_stacji",
    ])

    result = result.drop_duplicates(subset=["kod_stacji"])
    result = result.reset_index(drop=True)

    unsolved = unsolved.drop_duplicates(subset=["kod_stacji"])
    unsolved = unsolved.reset_index(drop=True)

    result["miejscowosc_id"] = result["miejscowosc_id"].astype(int)

    # Usuwamy techniczne kolumny pomocnicze, których nie ma w tabeli stacja.
    result = result.drop(columns=[
        "wojewodztwo_miejscowosc",
        "wojewodztwo_nazwa",
        "miejscowosc_nazwa",
    ])

    # Zamiana pd.NA / NaN na None pod SQL.
    result = result.astype(object).where(pd.notna(result), None)
    solved = result

    return {
        "solved": solved,
        "unsolved": unsolved
    }
'''

def normalize_simc_map(simc_map: dict) -> dict[str, int]:
    """
    Normalizuje mapę:

        kod_simc -> miejscowosc_id

    Dzięki temu kod SIMC zawsze ma 7 znaków, np.:
        97725 -> "0097725"
    """

    result = {}

    for kod_simc in simc_map:
        if pd.isna(kod_simc):
            continue

        kod_simc_text = str(kod_simc).strip()

        if kod_simc_text.endswith(".0"):
            kod_simc_text = kod_simc_text[:-2]

        kod_simc_text = kod_simc_text.zfill(7)

        result[kod_simc_text] = simc_map[kod_simc]

    return result

def apply_stacje_miejscowosci_corrections(
    dataframe_to_correct_manually: dict[str, pd.DataFrame],
    simc_map: dict[str, int],
    path_korekty: Path,
    only_auto: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    Etap 2.

    Nakłada ręczne korekty:

        kod_stacji -> kod_simc -> miejscowosc_id

    Ta funkcja NIE wywołuje API.
    Tylko używa gotowego pliku korekt.
    """

    if "all" not in dataframe_to_correct_manually:
        raise ValueError("prepared_result musi zawierać klucz 'all'.")

    result = dataframe_to_correct_manually["all"].copy()

    if not Path(path_korekty).exists():
        raise FileNotFoundError(f"Nie znaleziono pliku korekt: {path_korekty}")

    korekty_df = pd.read_csv(
        path_korekty,
        sep=";",
        dtype=str,
    )

    if "kod_stacji" not in korekty_df.columns:
        raise ValueError(
            f"Plik korekt nie ma kolumny 'kod_stacji'. "
            f"Dostępne kolumny: {list(korekty_df.columns)}"
        )

    if "proponowany_kod_simc" not in korekty_df.columns and "wybrany_kod_simc" not in korekty_df.columns:
        raise ValueError(
            "Plik korekt musi mieć kolumnę 'proponowany_kod_simc' "
            "albo 'wybrany_kod_simc'."
        )

    if only_auto and "uzyc_automatycznie" in korekty_df.columns:
        korekty_df["uzyc_automatycznie"] = (
            korekty_df["uzyc_automatycznie"]
            .astype("string")
            .str.strip()
            .str.upper()
        )

        korekty_df = korekty_df[
            korekty_df["uzyc_automatycznie"] == "TAK"
        ].copy()

    korekty_df["kod_stacji"] = (
        korekty_df["kod_stacji"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
    )

    korekty_df["kod_simc_do_uzycia"] = pd.NA

    if "proponowany_kod_simc" in korekty_df.columns:
        proponowany = (
            korekty_df["proponowany_kod_simc"]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
            .replace("None", pd.NA)
            .replace("nan", pd.NA)
            .replace("NaN", pd.NA)
        )

        korekty_df["kod_simc_do_uzycia"] = proponowany

    if "wybrany_kod_simc" in korekty_df.columns:
        wybrany = (
            korekty_df["wybrany_kod_simc"]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
            .replace("None", pd.NA)
            .replace("nan", pd.NA)
            .replace("NaN", pd.NA)
        )

        korekty_df["kod_simc_do_uzycia"] = wybrany.fillna(
            korekty_df["kod_simc_do_uzycia"]
        )

    korekty_df = korekty_df.dropna(subset=[
        "kod_stacji",
        "kod_simc_do_uzycia",
    ])

    korekty_df["kod_simc_do_uzycia"] = (
        korekty_df["kod_simc_do_uzycia"]
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(7)
    )

    kod_stacji_to_kod_simc = {}

    for _, row in korekty_df.iterrows():
        kod_stacji = row["kod_stacji"]
        kod_simc = row["kod_simc_do_uzycia"]

        kod_stacji_to_kod_simc[kod_stacji] = kod_simc

    simc_map_normalized = {}

    for kod_simc in simc_map:
        if pd.isna(kod_simc):
            continue

        kod_simc_text = str(kod_simc).strip()

        if kod_simc_text.endswith(".0"):
            kod_simc_text = kod_simc_text[:-2]

        kod_simc_text = kod_simc_text.zfill(7)

        simc_map_normalized[kod_simc_text] = simc_map[kod_simc]

    result["kod_simc_z_korekty"] = result["kod_stacji"].map(kod_stacji_to_kod_simc)
    result["miejscowosc_id_z_korekty"] = (
        result["kod_simc_z_korekty"]
        .map(simc_map_normalized)
    )

    kod_stacji_to_uwagi = {}

    if "uwagi" in korekty_df.columns:
        for _, row in korekty_df.iterrows():
            kod_stacji = row["kod_stacji"]
            uwagi = row["uwagi"]

            if pd.notna(uwagi):
                kod_stacji_to_uwagi[kod_stacji] = str(uwagi).strip()

    result["uwagi_z_korekty"] = result["kod_stacji"].map(kod_stacji_to_uwagi)

    mask_korekta = (
        result["miejscowosc_id"].isna()
        & result["miejscowosc_id_z_korekty"].notna()
    )

    result.loc[mask_korekta, "miejscowosc_id"] = result.loc[
        mask_korekta,
        "miejscowosc_id_z_korekty"
    ]

    result.loc[mask_korekta, "uwagi"] = result.loc[
        mask_korekta,
        "uwagi_z_korekty"
    ]

    result.loc[mask_korekta, "problem_type"] = None
    result.loc[mask_korekta, "problem_reason"] = "resolved_by_manual_correction"

    corrections_applied = result[mask_korekta].copy()

    mask_korekta_nie_zmapowana = (
        result["miejscowosc_id"].isna()
        & result["kod_simc_z_korekty"].notna()
        & result["miejscowosc_id_z_korekty"].isna()
    )

    corrections_not_mapped = result[mask_korekta_nie_zmapowana].copy()

    all_df = result.copy()

    cleaned = result[
        result["miejscowosc_id"].notna()
        & result["kod_stacji"].notna()
        & result["nazwa_stacji"].notna()
    ].copy()

    cleaned["miejscowosc_id"] = cleaned["miejscowosc_id"].astype(int)

    cleaned = cleaned.drop(columns=[
        "source_index",
        "wojewodztwo_miejscowosc",
        "wojewodztwo_nazwa",
        "miejscowosc_nazwa",
        "problem_type",
        "problem_reason",
        "woj",
        "powiat",
        "gmina",
        "rodzaj_gminy",
        "mozliwe_miejscowosci",
        "ambiguous_miejscowosc_ids",
        "kod_simc_z_korekty",
        "miejscowosc_id_z_korekty",
        "uwagi_z_korekty"
    ], errors="ignore")

    cleaned = cleaned.astype(object).where(pd.notna(cleaned), None)

    errors = result[result["miejscowosc_id"].isna()].copy()

    unsolved_geo = errors[
        errors["problem_type"] == "unsolved_geo"
    ].copy()

    unsolved_amb = errors[
        errors["problem_type"] == "unsolved_amb"
    ].copy()

    # ------------------------------------------------------------
    # ZAPIS CSV — IMPORTANT + DIAGNOSTICS
    # ------------------------------------------------------------

    important_dir = path_korekty.parent
    diagnostics_dir = important_dir.parent / "Diagnostics"

    important_dir.mkdir(parents=True, exist_ok=True)
    diagnostics_dir.mkdir(parents=True, exist_ok=True)

    # WAŻNE: finalne stacje po korektach.
    cleaned.to_csv(
        important_dir / "stacje_cleaned_after_corrections.csv",
        sep=";",
        index=False,
        encoding="utf-8-sig",
    )

    # WAŻNE: jeśli coś nadal nie weszło, tu będzie lista.
    errors.to_csv(
        important_dir / "stacje_errors_after_corrections.csv",
        sep=";",
        index=False,
        encoding="utf-8-sig",
    )

    # DIAGNOSTYKA: co dokładnie poprawiła mapa korekt.
    corrections_applied.to_csv(
        diagnostics_dir / "stacje_corrections_applied.csv",
        sep=";",
        index=False,
        encoding="utf-8-sig",
    )

    # DIAGNOSTYKA: korekta miała kod SIMC, ale nie udało się przełożyć na miejscowosc_id.
    corrections_not_mapped.to_csv(
        diagnostics_dir / "stacje_corrections_not_mapped.csv",
        sep=";",
        index=False,
        encoding="utf-8-sig",
    )

    # DIAGNOSTYKA: podział pozostałych błędów.
    unsolved_geo.to_csv(
        diagnostics_dir / "unsolved_geo_after_corrections.csv",
        sep=";",
        index=False,
        encoding="utf-8-sig",
    )

    unsolved_amb.to_csv(
        diagnostics_dir / "unsolved_amb_after_corrections.csv",
        sep=";",
        index=False,
        encoding="utf-8-sig",
    )

    print(f"Wczytano korekty: {len(korekty_df)}")
    print(f"Zastosowano korekty: {int(mask_korekta.sum())}")
    print(f"Korekty z kodem SIMC, ale bez miejscowosc_id: {len(corrections_not_mapped)}")
    print(f"cleaned po korektach: {len(cleaned)}")
    print(f"errors po korektach: {len(errors)}")
    print(f"Zapisano ważne pliki do: {important_dir}")
    print(f"Zapisano diagnostykę do: {diagnostics_dir}")

    return {
        "all": all_df,
        "cleaned": cleaned,
        "errors": errors,
        "unsolved_geo": unsolved_geo,
        "unsolved_amb": unsolved_amb,
        "corrections_applied": corrections_applied,
        "corrections_not_mapped": corrections_not_mapped,
    }

def prepare_stacje_from_metadata(
    stacje_df: pd.DataFrame,
    miejscowosc_map: dict[tuple[str, str], int],
    simc_map: dict[str, int],
    path_korekty: Path,
) -> dict[str, pd.DataFrame]:
    """
    Etap 1.

    Robi:
        1. czyszczenie STACJE,
        2. mapowanie po (wojewodztwo, miejscowosc),
        3. mapowanie przez GUGiK/ULDK,
        4. próbę naprawy przez klucz bez rodzaj_gminy,
        5. zapisuje problemy do path_korekty.

    NIE używa ręcznego pliku korekt.
    """

    _ = simc_map

    stacje_df = stacje_df.copy()

    required_columns = [
        "Kod stacji",
        "Kod międzynarodowy",
        "Nazwa stacji",
        "Stary Kod stacji \n(o ile inny od aktualnego)",
        "Data uruchomienia",
        "Data zamknięcia",
        "Typ stacji",
        "Typ obszaru",
        "Rodzaj stacji",
        "Województwo",
        "Miejscowość",
        "Adres",
        "WGS84 φ N",
        "WGS84 λ E",
    ]

    missing_columns = set(required_columns) - set(stacje_df.columns)

    if missing_columns:
        raise ValueError(
            f"Arkusz STACJE nie ma wymaganych kolumn: {missing_columns}. "
            f"Dostępne kolumny: {list(stacje_df.columns)}"
        )

    result = pd.DataFrame(index=stacje_df.index)
    result["source_index"] = stacje_df.index

    woj = (
        stacje_df["Województwo"]
        .astype("string")
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
    )

    miejscowosc = (
        stacje_df["Miejscowość"]
        .astype("string")
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
    )

    result["wojewodztwo_miejscowosc"] = list(zip(woj, miejscowosc))
    result["miejscowosc_id"] = result["wojewodztwo_miejscowosc"].map(miejscowosc_map)

    result["kod_stacji"] = (
        stacje_df["Kod stacji"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["kod_miedzynarodowy"] = (
        stacje_df["Kod międzynarodowy"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["nazwa_stacji"] = (
        stacje_df["Nazwa stacji"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["stary_kod_stacji"] = (
        stacje_df["Stary Kod stacji \n(o ile inny od aktualnego)"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["data_uruchomienia"] = pd.to_datetime(
        stacje_df["Data uruchomienia"],
        errors="coerce",
    ).dt.date.to_numpy()

    result["data_zamkniecia"] = pd.to_datetime(
        stacje_df["Data zamknięcia"],
        errors="coerce",
    ).dt.date.to_numpy()

    result["typ_stacji"] = (
        stacje_df["Typ stacji"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["typ_obszaru"] = (
        stacje_df["Typ obszaru"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["rodzaj_stacji"] = (
        stacje_df["Rodzaj stacji"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["adres"] = (
        stacje_df["Adres"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["adres"] = (
        stacje_df["Adres"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .to_numpy()
    )

    result["uwagi"] = None

    result["wgs84_fi_n"] = pd.to_numeric(
        stacje_df["WGS84 φ N"],
        errors="coerce",
    ).to_numpy()

    result["wgs84_lambda_e"] = pd.to_numeric(
        stacje_df["WGS84 λ E"],
        errors="coerce",
    ).to_numpy()

    result["wojewodztwo_nazwa"] = woj.to_numpy()
    result["miejscowosc_nazwa"] = miejscowosc.to_numpy()

    # Kolumny diagnostyczne.
    result["problem_type"] = None
    result["problem_reason"] = None
    result["woj"] = None
    result["powiat"] = None
    result["gmina"] = None
    result["rodzaj_gminy"] = None
    result["mozliwe_miejscowosci"] = None
    result["ambiguous_miejscowosc_ids"] = None

    engine = get_engine()

    miejscowosc_full_map = get_woj_pow_gm_rg_nazwa_to_miejscowosc_id_map(
        engine=engine,
        schema="gios",
    )

    partial_map_result = get_woj_pow_gm_nazwa_to_miejscowosc_id_map(
        engine=engine,
        schema="gios",
    )

    miejscowosc_partial_map = partial_map_result["resolved_map"]
    miejscowosc_partial_ambiguous_map = partial_map_result["ambiguous_map"]

    cache_path = Path("Data") / "Temp" / "gugik_stacje_cache.csv"

    initial_solved_count = int(result["miejscowosc_id"].notna().sum())
    api_exact_success_count = 0
    zly_rodzaj_rozwiazany = 0

    missing_indexes = result[result["miejscowosc_id"].isna()].index

    for index in missing_indexes:
        row = result.loc[index]

        lat = row["wgs84_fi_n"]
        lon = row["wgs84_lambda_e"]

        if pd.isna(lat) or pd.isna(lon) or lat == -999 or lon == -999:
            mozliwe_miejscowosci = get_miejscowosci_candidates_for_woj_and_name(
                engine=engine,
                wojewodztwo_nazwa=row["wojewodztwo_nazwa"],
                miejscowosc_nazwa=row["miejscowosc_nazwa"],
                schema="gios",
            )

            result.at[index, "problem_type"] = "unsolved_geo"
            result.at[index, "problem_reason"] = "brak_poprawnych_wspolrzednych"
            result.at[index, "mozliwe_miejscowosci"] = mozliwe_miejscowosci

            continue

        time.sleep(0.1)

        response = extract_data_from_GUGiK_API(
            lat=lat,
            lon=lon,
        )

        if response is None:
            result.at[index, "problem_type"] = "unsolved_geo"
            result.at[index, "problem_reason"] = "brak_odpowiedzi_api"

            continue

        parsed = parse_uldk_response_to_terc_and_save(
            response=response,
            kod_stacji=row["kod_stacji"],
            wgs84_fi_n=row["wgs84_fi_n"],
            wgs84_lambda_e=row["wgs84_lambda_e"],
            cache_path=cache_path,
        )

        if parsed is None:
            result.at[index, "problem_type"] = "unsolved_geo"
            result.at[index, "problem_reason"] = "api_nie_zwrocilo_teryt"

            continue

        woj_kod, powiat_kod, gmina_kod, rodzaj_gminy = parsed

        result.at[index, "woj"] = woj_kod
        result.at[index, "powiat"] = powiat_kod
        result.at[index, "gmina"] = gmina_kod
        result.at[index, "rodzaj_gminy"] = rodzaj_gminy

        nazwa_miejscowosci = str(row["miejscowosc_nazwa"]).strip().upper()

        full_key = (
            woj_kod,
            powiat_kod,
            gmina_kod,
            rodzaj_gminy,
            nazwa_miejscowosci,
        )

        miejscowosc_id = miejscowosc_full_map.get(full_key)

        if miejscowosc_id is not None:
            result.at[index, "miejscowosc_id"] = miejscowosc_id
            api_exact_success_count += 1

            continue

        partial_key = (
            woj_kod,
            powiat_kod,
            gmina_kod,
            nazwa_miejscowosci,
        )

        miejscowosc_id = miejscowosc_partial_map.get(partial_key)

        if miejscowosc_id is not None:
            result.at[index, "miejscowosc_id"] = miejscowosc_id
            zly_rodzaj_rozwiazany += 1

            continue

        ambiguous_ids = miejscowosc_partial_ambiguous_map.get(partial_key)

        mozliwe_miejscowosci = get_miejscowosci_candidates_for_gmina(
            engine=engine,
            woj=woj_kod,
            powiat=powiat_kod,
            gmina=gmina_kod,
            rodzaj_gminy=rodzaj_gminy,
            schema="gios",
        )

        result.at[index, "problem_type"] = "unsolved_amb"
        result.at[index, "mozliwe_miejscowosci"] = mozliwe_miejscowosci

        if ambiguous_ids is not None:
            result.at[index, "problem_reason"] = (
                f"ambiguous_po_usunieciu_rodzaju_gminy_dla_klucza_{partial_key}"
            )
            result.at[index, "ambiguous_miejscowosc_ids"] = str(ambiguous_ids)
        else:
            result.at[index, "problem_reason"] = (
                f"brak_miejscowosc_id_dla_klucza_{full_key}"
            )

    all_df = result.copy()

    cleaned = result[
        result["miejscowosc_id"].notna()
        & result["kod_stacji"].notna()
        & result["nazwa_stacji"].notna()
    ].copy()

    cleaned["miejscowosc_id"] = cleaned["miejscowosc_id"].astype(int)

    cleaned = cleaned.drop(columns=[
        "source_index",
        "wojewodztwo_miejscowosc",
        "wojewodztwo_nazwa",
        "miejscowosc_nazwa",
        "problem_type",
        "problem_reason",
        "woj",
        "powiat",
        "gmina",
        "rodzaj_gminy",
        "mozliwe_miejscowosci",
        "ambiguous_miejscowosc_ids",
    ], errors="ignore")

    cleaned = cleaned.astype(object).where(pd.notna(cleaned), None)

    errors = result[result["miejscowosc_id"].isna()].copy()

    unsolved_geo = errors[
        errors["problem_type"] == "unsolved_geo"
    ].copy()

    unsolved_amb = errors[
        errors["problem_type"] == "unsolved_amb"
    ].copy()

    # ------------------------------------------------------------
    # ZAPIS CSV — IMPORTANT + DIAGNOSTICS
    # ------------------------------------------------------------

    important_dir = path_korekty.parent
    diagnostics_dir = important_dir.parent / "Diagnostics"

    important_dir.mkdir(parents=True, exist_ok=True)
    diagnostics_dir.mkdir(parents=True, exist_ok=True)

    # WAŻNE: główny plik problemów do przygotowania korekt.
    errors.to_csv(
        path_korekty,
        sep=";",
        index=False,
        encoding="utf-8-sig",
    )

    # DIAGNOSTYKA: pomocnicze podziały błędów.
    unsolved_geo.to_csv(
        diagnostics_dir / "unsolved_geo.csv",
        sep=";",
        index=False,
        encoding="utf-8-sig",
    )

    unsolved_amb.to_csv(
        diagnostics_dir / "unsolved_amb.csv",
        sep=";",
        index=False,
        encoding="utf-8-sig",
    )

    cleaned.to_csv(
        diagnostics_dir / "stacje_cleaned_auto.csv",
        sep=";",
        index=False,
        encoding="utf-8-sig",
    )

    print(f"Stacje rozwiązane początkowo: {initial_solved_count}")
    print(f"Stacje rozwiązane przez pełny klucz API: {api_exact_success_count}")
    print(f"Stacje rozwiązane przez korektę rodzaju gminy: {zly_rodzaj_rozwiazany}")
    print(f"cleaned: {len(cleaned)}")
    print(f"errors: {len(errors)}")
    print(f"unsolved_geo: {len(unsolved_geo)}")
    print(f"unsolved_amb: {len(unsolved_amb)}")
    print(f"Zapisano problemy do: {path_korekty}")
    print(f"Zapisano diagnostykę do: {diagnostics_dir}")

    return {
        "all": all_df,
        "cleaned": cleaned,
        "errors": errors,
        "unsolved_geo": unsolved_geo,
        "unsolved_amb": unsolved_amb,
    }

def prepare_text_column(
    df: pd.DataFrame,
    source_column: str,
    target_column: str,
    drop_nulls: bool = True,
    drop_duplicates: bool = True,
    uppercase: bool = False,
) -> pd.DataFrame:
    """
    Przygotowuje jedną kolumnę tekstową:
    - wybiera source_column z df,
    - zamienia na string,
    - usuwa spacje z początku/końca,
    - opcjonalnie zamienia puste stringi na NA,
    - opcjonalnie usuwa NULL-e i duplikaty,
    - zwraca DataFrame z jedną kolumną target_column.
    """

    result = df[[source_column]].copy()

    result[target_column] = (
        result[source_column]
        .astype("string")
        .str.strip()
    )

    if uppercase:
        result[target_column] = result[target_column].str.upper()

    result[target_column] = result[target_column].replace("", pd.NA)

    result = result[[target_column]]

    if drop_nulls:
        result = result.dropna(subset=[target_column])

    if drop_duplicates:
        result = result.drop_duplicates(subset=[target_column])

    return result.reset_index(drop=True)

def prepare_numeric_column(
    df: pd.DataFrame,
    source_column: str,
    target_column: str,
    drop_nulls: bool = True,
    drop_duplicates: bool = True,
) -> pd.DataFrame:
    """
    Przygotowuje jedną kolumnę numeryczną:
    - wybiera source_column,
    - konwertuje na liczbę,
    - błędne wartości zamienia na NaN,
    - opcjonalnie usuwa NULL-e i duplikaty.
    """

    result = df[[source_column]].copy()

    result[target_column] = pd.to_numeric(
        result[source_column],
        errors="coerce"
    )

    result = result[[target_column]]

    if drop_nulls:
        result = result.dropna(subset=[target_column])

    if drop_duplicates:
        result = result.drop_duplicates(subset=[target_column])

    return result.reset_index(drop=True)

def prepare_wojewodztwa_df(df: pd.DataFrame) -> pd.DataFrame:
    woj_df = (
        df[["Województwo"]]
        .dropna()
        .drop_duplicates()
        .copy()
    )

    woj_df["nazwa_wojewodztwa"] = (
        woj_df["Województwo"]
        .astype(str)
        .str.strip()
    )
    return woj_df[["nazwa_wojewodztwa"]]