from datetime import date
import os
from shutil import copy2


def save_frozen_wd5_data(full_bw_flat_df, force_freeze: bool = False):
    frozen_data_path = "../../data/03_Group_1_2/FTE Validation - Frozen datasets"
    today = date.today()
    day = today.day

    if force_freeze:
        day = 5

    if day < 5:
        print("Day 5 has not been reached yet, WD5 Frozen dataset has not been created")
    elif day < 10:
        year = today.year
        curr_month = today.month
        date_search_prefix = str(year) + "-" + str(f'{curr_month:02d}')
        frozen_data_exists = False
        frozen_file = None
        for file in os.listdir(frozen_data_path):
            if file.startswith(date_search_prefix):
                frozen_data_exists = True
                frozen_file = file
        if frozen_data_exists:
            print(f"This month's data has already been saved in file {frozen_file}")
        else:
            print("WD5 Frozen dataset is being generated...")

            actual_month_check = full_bw_flat_df.copy()
            actual_month_check.to_excel(frozen_data_path + f"/{today} Frozen data for FTE Validation.xlsx")
            copy2(frozen_data_path + f"/{today} Frozen data for FTE Validation.xlsx",
                  "C:/Users/78595\OneDrive - Grundfos/Desktop/Backup/Recurrings backup/WD5 Frozen Datasets")
            print("WD5 Frozen dataset is done.")
    else:
        print("Day 5 has been too long ago, WD5 Frozen dataset has not been created")