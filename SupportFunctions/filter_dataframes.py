import pandas as pd
legal_company_field_name = 'Legal Company Legal Company Code'
leaving_date_field_name = 'Employment Details Termination Date'


def filter_dataframe_on_legal_company_and_field_name(dataframe, company: list, field_list: list) -> pd.DataFrame:
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Invalid type: dataframe argument must be of type DataFrame")
    if not isinstance(company, list):
        raise TypeError("Invalid type: company argument must be of type List")
    if not isinstance(field_list, list):
        raise TypeError("Invalid type: field_list argument must be of type List")

    filtered_dataframe = dataframe[(dataframe[legal_company_field_name]).isin(company)][field_list]

    return filtered_dataframe


def filter_turnover_dataframe_on_year_and_month(dataframe: pd.DataFrame, company: list, field_list: list, year: int, month: int) -> pd.DataFrame:
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Invalid type: dataframe argument must be of type DataFrame")
    if not isinstance(company, list):
        raise TypeError("Invalid type: company argument must be of type List")
    if not isinstance(field_list, list):
        raise TypeError("Invalid type: field_list argument must be of type List")
    if not isinstance(year, int):
        raise TypeError("Invalid type: year argument must be of type Integer")
    if not isinstance(month, int):
        raise TypeError("Invalid type: month argument must be of type Integer")

    filtered_dataframe = dataframe[(dataframe[legal_company_field_name]).isin(company) &
                                   (dataframe[leaving_date_field_name].dt.year == year) &
                                   (dataframe[leaving_date_field_name].dt.month == month)][field_list]
    return filtered_dataframe











