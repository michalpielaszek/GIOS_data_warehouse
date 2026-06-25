from Scripts.ETL.Extract.data_exctract import find_input_files, show_preview, load_raw_excels
from Pipeline.pipeline import run_pipeline
from Scripts.Database.connection import test_connection

import pandas as pd

def import_data(input_folder):
    paths = find_input_files(
        input_folder,
        recursive=False
    )

    print("Znalezione pliki:")
    for path in paths:
        print(f"- {path}")

    raw_data = load_raw_excels(
        paths,
        max_files_per_zip=3
    )

    i = 10
    for item in raw_data.items():
        if i >= 0:
            print(f"Key: {item[0]}")
            print(f"Value: {item[1].head(10)}")
        else:
            break


if __name__ == "__main__":
    # input_base = "Data/Raw"

    # df = import_data(
    #     input_folder=input_base + "/2024"
    # )

    run_pipeline()
    #test_connection()