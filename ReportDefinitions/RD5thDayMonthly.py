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


class RD5thDayMonthly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_gmh_tatabanya_turnover(self):
        turnover_data = self.dataprovider.get_turnover_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Employment Details Termination Date', 'Location Location Name']
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month -= 1
        company_filter = ['GMH']
        site_filter = 'HU-Tatabánya'

        gmh_turnover_df = filter_turnover_dataframe_on_year_and_month(turnover_data, company_filter, field_list, year,
                                                                      month)
        gmh_turnover_df = gmh_turnover_df[gmh_turnover_df['Location Location Name'] == site_filter]

        df_dict = {'Report': gmh_turnover_df}
        path = '03_TEMPLATE GMH Tatabánya Turnover.xlsx'
        o_name = 'GMH Tatabánya Turnover'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_dk_managers_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        manager_df = get_manager_data(headcount_data, ['Formal Name', 'Country/Region',
                                                       'Business Email Information Email Address'])

        soc_base_df = headcount_data[
            ['User/Employee ID', 'Formal Name', 'Country/Region', 'Manager User Sys ID']].copy()
        soc_base_df = soc_base_df.merge(manager_df, how='left')

        fields_to_group_by = ['Manager User Sys ID', 'Manager Formal Name', 'Manager Country/Region',
                              'Manager Business Email Information Email Address']
        dk_managers_soc_df = pd.DataFrame(soc_base_df[soc_base_df['Manager Country/Region'] == 'Denmark'].groupby(
            fields_to_group_by).size()).reset_index().rename({0: 'Headcount'}, axis=1)

        non_dk_managers_w_dk_emp_df = pd.DataFrame(
            soc_base_df[(soc_base_df['Manager Country/Region'] != 'Denmark') &
                        (soc_base_df['Country/Region'] == 'Denmark')].groupby(fields_to_group_by).size()).reset_index() \
            .rename({0: 'Headcount'}, axis=1)

        df_dict = {'DK managers': dk_managers_soc_df, '!DK managers w DK employees': non_dk_managers_w_dk_emp_df}
        path = '03_TEMPLATE DK Managers Report.xlsx'
        o_name = 'DK Managers Report'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_product_development_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['Initials', 'User/Employee ID', 'Formal Name', 'Group Band Code',
                      'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
                      'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                      'Location Location Name', 'Manager User Sys ID', 'Direct Manager', 'Cost Centre Cost Center Code',
                      'Job Title', 'Age in Years', 'Date of Birth', 'Seniority in Years',
                      'Employment Details Anniversary Date', 'Business Email Information Email Address',
                      'Contract Type', 'FTE']

        org_criteria = 50171667
        product_development_df = headcount_data[headcount_data['Level_4'] == org_criteria][field_list]

        df_dict = {'Report': product_development_df}
        path = '03_TEMPLATE Product Development Headcount.xlsx'
        o_name = 'Product Development Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gms_sum_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Legal Company Legal Company Name', 'Subarea']
        company_filter = ['GMS']

        gms_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, field_list)
        gms_df = gms_df.groupby(['Legal Company Legal Company Name', 'Subarea']).count().reset_index()

        df_dict = {'Report': gms_df}
        path = '03_TEMPLATE GMS Summary Headcount.xlsx'
        o_name = 'GMS Summary Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_dk_reduced_work_capacity_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        region_data = self.dataprovider.get_region_data()
        legal_company_filter = list(region_data[region_data['Country'] == 'Denmark']['Legal Company code'])
        field_list = ['User/Employee ID', 'Legal Company Legal Company Name', 'Disability Status']

        dk_rwc_df = headcount_data[(headcount_data['Legal Company Legal Company Code'].isin(legal_company_filter)) &
                                   (headcount_data['Disability Status'] == 'Disabled')][field_list].groupby(
            field_list[1:]).count().reset_index()

        df_dict = {'Report': dk_rwc_df}
        path = '03_TEMPLATE DK Reduced Work Capacity Report.xlsx'
        o_name = 'DK Reduced Work Capacity Report'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gms_leavers(self):
        turnover_data = self.dataprovider.get_turnover_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month -= 1
        company_filter = ['GMS']
        field_list = ['User/Employee ID', 'Formal Name', 'Manager User Sys ID', 'Direct Manager',
                      'Department Unit Department Unit Name', 'Employment Details Termination Date',
                      'Employment Details Anniversary Date', 'Event Reason']

        gms_turnover_df = filter_turnover_dataframe_on_year_and_month(turnover_data, company_filter, field_list, year,
                                                                      month)

        df_dict = {'Report': gms_turnover_df}
        path = '03_TEMPLATE GMS Leavers.xlsx'
        o_name = 'GMS Leavers'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_ind_mobility_sales_report(self):
        prev_df = self.dataprovider.get_previous_month_headcount_data()
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1

        orgs = ['IND Mobility', 'IND Sales APAC']
        newcomer_fields = ['User/Employee ID', 'Formal Name', 'Department Unit Department Unit Name',
                           'Cost Centre Cost Center Code', 'Job Role Job Role Name',
                           'Employment Details Anniversary Date', 'Manager User Sys ID', 'Direct Manager']

        base = headcount_data[headcount_data['Level_5_Name'].isin(orgs)][newcomer_fields].copy()

        # Newcomers
        newcomers = base[(base['Employment Details Anniversary Date'].dt.year == year)]

        # Transfers
        field_list = ['User/Employee ID', 'Formal Name', 'Level_5_Name', 'Department Unit Department Unit Name',
                      'Job Role Job Role Name', 'Group Band Code']
        transfers_base = extract_employee_data_changes(headcount_data, prev_df, field_list)
        transfers = transfers_base[(transfers_base['Prev_M_Level_5_Name'].isin(orgs)) |
                                   (transfers_base['Curr_M_Level_5_Name'].isin(orgs))]

        # Leavers
        lvl_5_org_criteria = [50172242, 50172246]

        turnover_fields = ['User/Employee ID', 'Formal Name', 'Department Unit Department Unit Name',
                           'Job Role Job Role Name', 'Group Band Code', 'Employment Details Termination Date',
                           'Reason for leaving', 'Event Reason', 'Manager User Sys ID', 'Direct Manager']
        turnover = turnover_data[turnover_data['Level_5'].isin(lvl_5_org_criteria)][turnover_fields].copy()

        df_dict = {'Newcomers': newcomers, 'Changes': transfers, 'Turnover': turnover}
        path = '03_TEMPLATE IND Mobility and Sales Report.xlsx'
        o_name = 'IND Mobility and Sales Report'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gmh_gms_gmo_gmr_gpl_newcomers_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1
        company_filter = ['GMH', 'GMS', 'GMO', 'GMR', 'GPL']
        newcomers_field_list = ['Global ID', 'Formal Name', 'Employment Details Anniversary Date',
                                'Legal Company Legal Company Code', 'Job Title', 'Job Role Job Role Name',
                                'Contract Type', 'Subarea', 'Employee Sub Group Name', 'Group Band Code',
                                'Manager User Sys ID', 'Direct Manager', 'STI Target %', 'STI Max %',
                                'HR Business Partner Job Relationships User ID',
                                'HR Business Partner Job Relationships Name']

        df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, newcomers_field_list)

        leavers_field_list = ['Global ID', 'Formal Name', 'Employment Details Termination Date',
                              'Legal Company Legal Company Code', 'Job Title', 'Job Role Job Role Name',
                              'Contract Type', 'Subarea', 'Employee Sub Group Name', 'Group Band Code',
                              'Manager User Sys ID', 'Direct Manager', 'STI Target %', 'STI Max %',
                              'HR Business Partner Job Relationships User ID',
                              'HR Business Partner Job Relationships Name']

        t_df = turnover_data[(turnover_data['Legal Company Legal Company Code'].isin(company_filter)) &
                             (turnover_data['Employment Details Termination Date'].dt.year == year)][leavers_field_list]

        template_path = '03_TEMPLATE GMH GMS GMO GMR GPL Newcomers and Turnover.xlsx'
        o_name = 'GMH GMS GMO GMR GPL Newcomers and Turnover'
        df_dict = {'Newcomers': df, 'Leavers': t_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name
