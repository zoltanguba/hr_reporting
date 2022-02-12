import pandas as pd


def get_manager_data(dataframe, field_list: list):
    managers_df = dataframe[['Manager User Sys ID']].copy().drop_duplicates()

    if 'User/Employee ID' in field_list:
        pass
    else:
        field_list.insert(0, 'User/Employee ID')

    managers_merged_df = managers_df.merge(dataframe[field_list],
                                           how='left',
                                           left_on='Manager User Sys ID',
                                           right_on='User/Employee ID')
    managers_merged_df = managers_merged_df.drop(['User/Employee ID'], axis=1)

    managers_merged_df.columns = ['Manager ' + column if column != 'Manager User Sys ID' else column for column in
                                  managers_merged_df.columns]
    if 'Manager User Sys ID' in field_list:
        managers_merged_df.rename({'Manager Manager User Sys ID_x': 'Manager User Sys ID',
                                   'Manager Manager User Sys ID_y': "Manager's Manager",
                                   'Manager Direct Manager': "Manager's manager name"}, axis=1, inplace=True)

    return managers_merged_df


