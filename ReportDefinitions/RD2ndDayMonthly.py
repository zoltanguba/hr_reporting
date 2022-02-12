from datetime import date
import pandas as pd
from ReportingSourceCode.ReportDefinitions.IReportDefinition import IReportDefinition
from ReportingSourceCode.DataProvider.IDataProvider import IDataProvider
from ReportingSourceCode.SupportFunctions.export_dataframes_to_excel import (export_df,
                                                                             export_df_with_header,
                                                                             export_df_with_header_and_index)
from ReportingSourceCode.SupportFunctions.filter_dataframes import filter_dataframe_on_legal_company_and_field_name
from ReportingSourceCode.SupportFunctions.decorators import measure_running_time
from ReportingSourceCode.SupportFunctions.get_manager_data import get_manager_data
import os


class RD2ndDayMonthly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_cn_seniority_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Employment Details Anniversary Date', 'Position Entry Date']
        legal_company_criteria = ['GSH', 'GCH', 'GCQ', 'GPC', 'GWC']
        cn_sen_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, legal_company_criteria, field_list)

        df_dict = {'Report': cn_sen_df}
        path = '03_TEMPLATE CN Seniority Report.xlsx'
        o_name = 'CN Seniority Report'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gtr_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Job Title', 'Group Band Code', 'Job Role Job Role Name',
                      'Manager User Sys ID', 'Direct Manager']
        legal_company_criteria = ['GTR']

        gtr_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, legal_company_criteria, field_list)

        df_dict = {'Report': gtr_df}
        path = '03_TEMPLATE GTR Headcount.xlsx'
        o_name = 'GTR Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_south_america_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        salary_data = self.dataprovider.get_salary_data()
        country_filter = ['Argentina', 'Peru', 'Colombia', 'Chile']
        field_list = ['User/Employee ID', 'Formal Name', 'Country/Region', 'Group Band Code', 'Manager User Sys ID',
                      'Direct Manager', 'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name',
                      'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                      'Employment Details Legal Date']
        salary_fields = ['User/Employee ID', 'Amount', 'Currency', 'Frequency']

        sa_df = headcount_data[headcount_data['Country/Region'].isin(country_filter)][field_list]
        salaries = salary_data[salary_fields]
        sa_df = sa_df.merge(salaries, how='left', on='User/Employee ID')

        df_dict = {'Report': sa_df}
        path = '03_TEMPLATE South America Headcount.xlsx'
        o_name = 'South America Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_americas_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Group Band Code', 'Legal Company Legal Company Code',
                      'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name', 'Job Role Job Role Name',
                      'Job Title', 'Department Unit Department Unit Name', 'STI Target %', 'STI Max %', 'Region 2']

        americas_hc = headcount_data[headcount_data['Region 2'] == 'AMERICAS'][field_list]

        df_dict = {'Report': americas_hc}
        path = '03_TEMPLATE Americas Headcount.xlsx'
        o_name = 'Americas Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_group_band_1_3_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        bands = ['01', '02', '03']
        fields = ['User/Employee ID', 'Formal Name', 'Group Band Code', 'Business Email Information Email Address']
        band_hc = headcount_data[headcount_data['Group Band Code'].isin(bands)][fields]

        df_dict = {'Report': band_hc}
        path = '03_TEMPLATE Group Band 1-3 Headcount.xlsx'
        o_name = 'Group Band 1-3 Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gas_cc_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name', 'FTE']
        legal_company_criteria = ['GTI', 'GAS']
        gas_cc_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, legal_company_criteria, field_list)

        df_dict = {'Report': gas_cc_df}
        path = '03_TEMPLATE GAS Cost Center Report.xlsx'
        o_name = 'GAS GTI Cost Center Report'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_cbs_sales_service_turnover(self):
        turnover_data = self.dataprovider.get_turnover_data()
        fields = ['User/Employee ID', 'Formal Name', 'Gender', 'Location Location Name', 'Job Title',
                  'Group Band Code', 'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                  'Direct Manager', 'Legal Company Legal Company Code', 'Employment Details Anniversary Date',
                  'Employment Details Termination Date', 'Level_3_Name', 'Level_5_Name', 'Level_5', 'Level_6_Name',
                  'Level_6']
        lvl_5_filter = [50172338, 50172414]
        lvl_6_filter = [50172365]

        df_merged = turnover_data[(turnover_data['Level_5'].isin(lvl_5_filter)) |
                                  (turnover_data['Level_6'].isin(lvl_6_filter))][fields]

        df_dict = {'Report': df_merged}
        path = '03_TEMPLATE CBS Sales & Service Turnover.xlsx'
        o_name = 'CBS Sales & Service Turnover'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gms_gse_summary_headcount(self):
        exp_data = self.dataprovider.get_expanded_ec_data()
        exp_data = exp_data[exp_data['Employee Status'] != 'Terminated']
        legal_company_criteria = ['GMS', 'GSE']
        field_list = ['User/Employee ID', 'Formal Name', 'Employee Status', 'Legal Company Legal Company Code',
                      'Job Title', 'Manager User Sys ID', 'Direct Manager',
                      'Department Unit Department Unit Code', 'Department Unit Department Unit Name', 'Contract Type']

        df = filter_dataframe_on_legal_company_and_field_name(exp_data, legal_company_criteria, field_list)

        summary_df = pd.crosstab(df['Legal Company Legal Company Code'], df['Employee Status']).reset_index()

        df_dict = {'Summary': summary_df, 'Headcount': df}
        path = '03_TEMPLATE GMS GSE Summary Headcount.xlsx'
        o_name = 'GMS GSE Summary Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_chro_monthly_newcomers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        year = date.today().year
        month = date.today().month - 1
        if month == 1:
            year = year - 1
            month = 12
        org_criteria = ['HR (CHRO)']
        field_list = ['User/Employee ID', 'Formal Name', 'Level_3_Name', 'Department Unit Department Unit Name',
                      'Legal Company Legal Company Code', 'Business Email Information Email Address',
                      'Employment Details Anniversary Date', 'Manager User Sys ID', 'Job Role Job Role Name',
                      'Job Title']

        chro_newcomers_df = headcount_data[(headcount_data['Level_3_Name']).isin(org_criteria) &
                                           (headcount_data['Employment Details Anniversary Date'].dt.year == year) &
                                           (headcount_data['Employment Details Anniversary Date'].dt.month == month)][
                            field_list]

        df_dict = {'Report': chro_newcomers_df}
        path = '03_TEMPLATE CHRO Monthly Newcomers.xlsx'
        o_name = 'CHRO Monthly Newcomers'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_global_hr_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        org_criterion = ['HR (CHRO)']
        fields = ['User/Employee ID', 'Formal Name', 'Level_3_Name', 'Department Unit Department Unit Name',
                  'Job Role Job Role Name', 'Manager User Sys ID', 'Direct Manager', 'Location Location Name',
                  'Employee Group']

        df = headcount_data[headcount_data['Level_3_Name'].isin(org_criterion)][fields]

        template_path = '03_TEMPLATE Global HR Headcount.xlsx'
        o_name = 'Global HR Headcount'
        df_dict = {'Report': df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name
