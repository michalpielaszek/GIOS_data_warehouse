import pandas as pd


def load_terc(path: str) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        sep=";",
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=True,
    )

    for col in ["WOJ", "POW", "GMI", "RODZ", "NAZWA", "NAZWA_DOD"]:
        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
            .replace("nan", pd.NA)
            .replace("None", pd.NA)
        )

    return df


def prepare_wojewodztwa_from_terc(terc_df: pd.DataFrame) -> pd.DataFrame:
    woj_df = terc_df[
        terc_df["WOJ"].notna()
        & terc_df["POW"].isna()
        & terc_df["GMI"].isna()
    ].copy()

    woj_df.reset_index(drop=True)

    result = pd.DataFrame()
    result["kod_wojewodztwa"] = woj_df["WOJ"]
    result["nazwa_wojewodztwa"] = woj_df["NAZWA"].str.upper()

    return result.drop_duplicates().reset_index(drop=True)


def prepare_powiaty_from_terc(
    terc_df: pd.DataFrame,
    woj_map: dict[str, int],
) -> pd.DataFrame:
    pow_df = terc_df[
        terc_df["WOJ"].notna()
        & terc_df["POW"].notna()
        & terc_df["GMI"].isna()
    ].copy()

    pow_df.reset_index(drop=True)

    result = pd.DataFrame()
    result["wojewodztwo_id"] = pow_df["WOJ"].map(woj_map)
    result["kod_powiatu"] = pow_df["POW"]
    result["nazwa_powiatu"] = pow_df["NAZWA"]
    result["typ_powiatu"] = pow_df["NAZWA_DOD"]

    missing = result[result["wojewodztwo_id"].isna()]
    if not missing.empty:
        raise ValueError(f"Powiaty bez wojewodztwo_id:\n{missing.head(20)}")

    return result.drop_duplicates().reset_index(drop=True)


def prepare_gminy_from_terc(
    terc_df: pd.DataFrame,
    powiat_map: dict[tuple[str, str], int],
) -> pd.DataFrame:
    gmi_df = terc_df[
        terc_df["WOJ"].notna()
        & terc_df["POW"].notna()
        & terc_df["GMI"].notna()
        & terc_df["RODZ"].notna()
        & terc_df["NAZWA"].notna()
        & terc_df["NAZWA_DOD"].notna()
    ].copy()

    # KLUCZOWA LINIA:
    # resetujemy indeks, żeby Pandas nie mieszał wartości przy przypisywaniu do result
    gmi_df = gmi_df.reset_index(drop=True)

    result = pd.DataFrame()

    result["powiat_key"] = list(zip(gmi_df["WOJ"], gmi_df["POW"]))
    result["powiat_id"] = result["powiat_key"].map(powiat_map)

    result["kod_gminy"] = gmi_df["GMI"].to_numpy()
    result["rodzaj_gminy"] = gmi_df["RODZ"].to_numpy()
    result["nazwa_gminy"] = gmi_df["NAZWA"].to_numpy()
    result["typ_gminy"] = gmi_df["NAZWA_DOD"].to_numpy()

    result = result.drop(columns=["powiat_key"])

    result = result.dropna(subset=[
        "powiat_id",
        "kod_gminy",
        "rodzaj_gminy",
        "nazwa_gminy",
        "typ_gminy",
    ])

    result = result.drop_duplicates(
        subset=[
            "powiat_id",
            "kod_gminy",
            "rodzaj_gminy",
        ]
    )

    result = result.reset_index(drop=True)

    result["powiat_id"] = result["powiat_id"].astype(int)

    return result