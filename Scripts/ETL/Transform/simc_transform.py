import pandas as pd


def load_simc(path: str) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        sep=";",
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=True,
    )

    required_columns = [
        "WOJ",
        "POW",
        "GMI",
        "RODZ_GMI",
        "NAZWA",
        "SYM",
    ]

    missing_columns = set(required_columns) - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Plik SIMC nie ma wymaganych kolumn: {missing_columns}. "
            f"Dostępne kolumny: {list(df.columns)}"
        )

    for col in required_columns:
        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
            .replace("nan", pd.NA)
            .replace("None", pd.NA)
        )

    return df


def prepare_miejscowosci_from_simc(
    simc_df: pd.DataFrame,
    gmina_map: dict[tuple[str, str, str, str], int],
) -> pd.DataFrame:
    simc_df = simc_df.copy()

    simc_df = simc_df.dropna(
        subset=[
            "WOJ",
            "POW",
            "GMI",
            "RODZ_GMI",
            "NAZWA",
            "SYM",
        ]
    )

    # Ujednolicenie formatów kodów TERYT/SIMC
    simc_df["WOJ"] = simc_df["WOJ"].astype("string").str.strip().str.zfill(2)
    simc_df["POW"] = simc_df["POW"].astype("string").str.strip().str.zfill(2)
    simc_df["GMI"] = simc_df["GMI"].astype("string").str.strip().str.zfill(2)
    simc_df["RODZ_GMI"] = simc_df["RODZ_GMI"].astype("string").str.strip()

    simc_df["NAZWA"] = simc_df["NAZWA"].astype("string").str.strip()
    simc_df["SYM"] = simc_df["SYM"].astype("string").str.strip().str.zfill(7)

    result = pd.DataFrame()

    result["gmina_key"] = list(
        zip(
            simc_df["WOJ"],
            simc_df["POW"],
            simc_df["GMI"],
            simc_df["RODZ_GMI"],
        )
    )

    print("Przykładowe klucze SIMC:")
    print(result["gmina_key"].head(20).to_list())

    print("Przykładowe klucze z bazy:")
    print(list(gmina_map.keys())[:20])

    result["gmina_id"] = result["gmina_key"].map(gmina_map)
    result["nazwa_miejscowosci"] = simc_df["NAZWA"]
    result["kod_simc"] = simc_df["SYM"]

    missing = result[result["gmina_id"].isna()]

    if not missing.empty:
        print("Uwaga: są miejscowości bez dopasowanego gmina_id.")
        print(missing.head(20))
        print(f"Liczba niedopasowanych rekordów: {len(missing)}")

    result = result.dropna(
        subset=[
            "gmina_id",
            "nazwa_miejscowosci",
            "kod_simc",
        ]
    )

    result = result.drop_duplicates(subset=["kod_simc"])

    result = result.drop(columns=["gmina_key"])

    result = result.reset_index(drop=True)

    result["gmina_id"] = result["gmina_id"].astype(int)

    result = result.astype(object).where(pd.notna(result), None)

    return result