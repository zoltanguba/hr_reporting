from datetime import date
import os
import pandas as pd

previous_month_prefix = 'Prev_M_'
current_month_prefix = 'Curr_M_'
first_fields = ['User/Employee ID', 'Formal Name']


def rename_and_merge_current_and_previous_datasets(fields: list, current_df: pd.DataFrame, previous_df: pd.DataFrame):
    full_field_list = first_fields + [field for field in fields if field not in first_fields]

    previous_month_narrowed = previous_df[full_field_list].copy()

    previous_month_narrowed.columns = [previous_month_prefix + column if column not in first_fields else column for
                                       column in previous_month_narrowed.columns]

    current_month_narrowed = current_df[full_field_list].copy()

    current_month_narrowed.columns = [current_month_prefix + column if column not in first_fields else column for column
                                      in current_month_narrowed.columns]

    merged_df = previous_month_narrowed.merge(current_month_narrowed, how='inner', on=first_fields)
    merged_df.fillna("None", inplace=True)

    return merged_df


def check_and_return_changes(fields_to_compare: list, merged_and_renamed_df: pd.DataFrame) -> pd.DataFrame:
    fields_to_compare = [field for field in fields_to_compare if field not in first_fields]

    for field in fields_to_compare:
        merged_and_renamed_df[field + ' Change'] = merged_and_renamed_df[current_month_prefix + field] != \
                                                   merged_and_renamed_df[previous_month_prefix + field]

    query_string = " | ".join(["`" + field + " Change` == True" for field in fields_to_compare])
    changes_df = merged_and_renamed_df.query(query_string)

    return changes_df


def extract_employee_data_changes(curr_data: pd.DataFrame, prev_data: pd.DataFrame,
                                   fields_to_compare: list) -> pd.DataFrame:
    curr_df = curr_data.copy()
    prev_df = prev_data.copy()

    merged_and_renamed_df = rename_and_merge_current_and_previous_datasets(fields_to_compare, curr_df, prev_df)
    changes_df = check_and_return_changes(fields_to_compare, merged_and_renamed_df)

    return changes_df










