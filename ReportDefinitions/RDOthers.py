from datetime import date
import pandas as pd
from ReportingSourceCode.ReportDefinitions.IReportDefinition import IReportDefinition
from ReportingSourceCode.DataProvider.IDataProvider import IDataProvider
from ReportingSourceCode.SupportFunctions.export_dataframes_to_excel import (export_df,
                                                                             export_df_with_header,
                                                                             export_df_with_header_and_index)
from ReportingSourceCode.SupportFunctions.filter_dataframes import (filter_dataframe_on_legal_company_and_field_name,
                                                                    filter_turnover_dataframe_on_year_and_month)
from ReportingSourceCode.SupportFunctions.decorators import measure_running_time
from ReportingSourceCode.SupportFunctions.get_manager_data import get_manager_data
import os
from ReportingSourceCode.SupportFunctions.extract_employee_data_changes import extract_employee_data_changes


class RDOthers(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_yearly_dk_diversity_report(self):
        headcount_data = self.dataprovider.get_headcount_data()

        company_filter = ['GOE', 'GBJ', 'GDK', 'GLL', 'GBB', 'GMA']
        fields = ['User/Employee ID', 'Formal Name', 'Gender', 'Nationality', 'Legal Company Legal Company Code',
                  'Legal Company Legal Company Name', 'Is People Manager Y/N', 'Employee Status']

        base_data = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, fields)
        base_data = base_data[base_data['Is People Manager Y/N'] == 'Yes']
        base_data['Nationality 2'] = base_data['Nationality'].apply(
            lambda x: "Danes" if x == "Denmark" else "Non-Danes")

        sum1 = pd.crosstab(
            [base_data['Legal Company Legal Company Code'], base_data['Legal Company Legal Company Name'],
             base_data['Is People Manager Y/N']], [base_data['Nationality 2']], margins='rows')

        sum2 = pd.crosstab(
            [base_data['Legal Company Legal Company Code'], base_data['Legal Company Legal Company Name'],
             base_data['Is People Manager Y/N']], [base_data['Nationality 2']], margins=True, normalize='index')
        sum2.rename({'Danes': 'Danes %', 'Non-Danes': 'Non-Danes %'}, axis=1, inplace=True)

        sum_fields = ['Legal Company Legal Company Code', 'Legal Company Legal Company Name', 'Is People Manager Y/N',
                      'All',
                      'Danes', 'Non-Danes', 'Danes %', 'Non-Danes %']

        output_df = pd.concat([sum1, sum2], axis=1).reset_index()[sum_fields]

        template_path = '03_TEMPLATE Yearly DK Diversity Report.xlsx'
        o_name = 'Yearly DK Diversity Report'
        df_dict = {'Report': output_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    def rec_health_and_safety_summary(self):
        headcount_data = self.dataprovider.get_headcount_data()
        hs_ass_path = 'C:/Users/78595/PycharmProjects/recurring_reports/ReportingSourceCode/data/dynamic/HS Assigned.csv'
        hs_comp_path = 'C:/Users/78595/PycharmProjects/recurring_reports/ReportingSourceCode/data/dynamic/HS Completed.csv'
        assignement_data = pd.read_csv(hs_ass_path)
        completion_data = pd.read_csv(hs_comp_path)

        assignment_users = list(assignement_data['User ID'])
        completed_users = list(completion_data['User ID'])

        fields = ['User/Employee ID', 'Level_3_Name', 'Level_4_Name', 'Legal Company Legal Company Code',
                  'HR Business Partner Job Relationships User ID', 'HR Business Partner Job Relationships Name']

        summary_data = headcount_data[headcount_data['User/Employee ID'].isin(assignment_users)][fields]
        summary_data['HSW Completion'] = summary_data['User/Employee ID'].apply(
            lambda x: 'Attended' if x in completed_users else 'Not Attended')

        df_dict = {'Data': summary_data}
        template_path = '03_TEMPLATE HSW Summary.xlsx'
        o_name = 'HSW Summary'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name













