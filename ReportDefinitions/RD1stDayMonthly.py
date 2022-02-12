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


class RD1stDayMonthly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_gmh_hc_turnover(self):
        exp_data = self.dataprovider.get_expanded_ec_data()
        turnover_data = self.dataprovider.get_turnover_data()
        field_list = ['User/Employee ID', 'First Name', 'Last Name', 'Employee Status',
                      'Job Family Name', 'Job Cluster Job Cluster Name', 'Job Role Job Role Name',
                      'Group Band Code', 'IPE IPE Code', 'Job Title', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Cost Centre Cost Center Code',
                      'Manager User Sys ID', 'Direct Manager', 'Contract End Date',
                      'Business Email Information Email Address', 'Employment Details Original Start Date', 'Subarea',
                      'Location Location Name', 'STI Target %', 'STI Max %', 'FTE', 'Event Date']
        # Headcount
        legal_company_code = 'GMH'
        gmh_base_df = exp_data[(exp_data['Legal Company Legal Company Code'] == legal_company_code) &
                               (exp_data['Employee Status'] != 'Terminated')][field_list]

        active_df = gmh_base_df[gmh_base_df['Employee Status'] == 'Active'].copy()
        inactive_df = gmh_base_df[gmh_base_df['Employee Status'] == 'On Leave'].copy()

        # Turnover
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Employment Details Termination Date', 'Reason for leaving', 'Event Reason']

        gmh_turnover_df = filter_turnover_dataframe_on_year_and_month(turnover_data, [legal_company_code], field_list,
                                                                      year, month)

        df_dict = {'Active': active_df, 'Inactive': inactive_df, 'Turnover': gmh_turnover_df}
        path = '03_TEMPLATE GMH HC + Turnover report.xlsx'
        o_name = 'GMH Headcount + Turnover'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gmh_turnover(self):
        turnover_data = self.dataprovider.get_turnover_data()
        field_list = ['User/Employee ID', 'Last Name', 'First Name', 'Legal Company Legal Company Code',
                      'Location Location Name', 'Subarea', 'Job Role Job Role Name', 'Job Title']
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1
        company_filter = ['GMH']

        gmh_turnover_df = filter_turnover_dataframe_on_year_and_month(turnover_data, company_filter, field_list, year,
                                                                      month)

        df_dict = {'Report': gmh_turnover_df}
        path = '03_TEMPLATE GMH Turnover.xlsx'
        o_name = 'GMH Turnover'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_reentry(self):
        rehire_df = self.dataprovider.get_rehire_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Event', 'Subarea', 'Global ID',
                      'Employment Details Anniversary Date',
                      'Position Entry Date', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Employee Group', 'Location Location Name',
                      'Employee Sub Group Name', 'Group Band Code', 'Job Family Name', 'Job Cluster Job Cluster Name',
                      'Job Role Job Role Name', 'Legal Company Legal Company Code', 'Organisational Company Code',
                      'Legal Company Legal Company Name', 'Manager User Sys ID', 'Direct Manager', 'Job Title',
                      'Is People Manager Y/N', 'Level_3_Name', 'Level_4_Name', 'Level_5_Name', 'Level_6_Name',
                      'STI Target %', 'STI Max %', 'Position Entry Date', 'Employment Details Last Date Worked']
        month = date.today().month
        if month == 1:
            month = 12
        else:
            month = month - 1

        reentry_full_df = rehire_df[rehire_df['Position Entry Date'].dt.month == month][field_list]

        df_dict = {'Report': reentry_full_df}
        path = '03_TEMPLATE Re-entry.xlsx'
        o_name = 'Global Re-entry'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_namreg_samreg_emp_list(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Legal Company Legal Company Name', 'Cost Centre Cost Center Code',
                      'Cost Centre Cost Center Name', 'STI Target %', 'STI Max %']
        company_filter = ['GCA', 'GMX', 'GMU', 'GCI', 'GPU', 'YCC', 'GCB', 'BGA', 'GBR', 'BGC', 'MXP', 'PPU',
                          'GCO', 'ENQ', 'GPE']

        base_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, field_list)

        template_path = '03_TEMPLATE NAMREG and SAMREG employee list.xlsx'
        o_name = 'NAMREG and SAMREG employee list'
        df_dict = {'Report': base_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_ame_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Department Unit Department Unit Name',
                      'Legal Company Legal Company Code', 'Legal Company Legal Company Name', 'FTE']
        org_unit_criteria = [50171625]

        ame_df = headcount_data[(headcount_data['Level_4']).isin(org_unit_criteria)][field_list]

        df_dict = {'Report': ame_df}
        template_path = '03_TEMPLATE AME Headcount.xlsx'
        o_name = 'AME Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_is_employee_list(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Initials',
                      'Job Title', 'Job Role Job Role Name', 'Legal Company Legal Company Code',
                      'Legal Company Legal Company Name', 'Country/Region']

        org_unit_criteria = [50171635]

        is_df = headcount_data[(headcount_data['Level_4']).isin(org_unit_criteria)][field_list]

        template_path = '03_TEMPLATE IS employee list.xlsx'
        o_name = 'IS Employees List'
        df_dict = {'Report': is_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gsh_gch_gcq_ghk_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Initials', 'Legal Company Legal Company Code',
                      'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                      'Manager User Sys ID', 'Direct Manager', 'Job Title', 'Job Role Job Role Name', 'Group Band Code',
                      'Location Location Name', 'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name']

        legal_company_criteria = ['GSH', 'GCH', 'GCQ', 'GHK']
        output_dict = {}

        for company in legal_company_criteria:
            df = filter_dataframe_on_legal_company_and_field_name(headcount_data, [company], field_list)
            output_dict[company] = df

        template_path = '03_TEMPLATE gsh_gch_gcq_ghk_hc.xlsx'
        o_name = 'GSH GCH GCQ GHK HC'
        df_dict = {'GSH': output_dict['GSH'], 'GCH': output_dict['GCH'], 'GCQ': output_dict['GCQ'],
                   'GHK': output_dict['GHK']}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_executive(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'First Name', 'Last Name', 'Business Email Information Email Address',
                      'Country/Region', 'Legal Company Legal Company Code', 'Global ID', 'Level_3_Name']

        executive_base_df = headcount_data[(~headcount_data['Level_1'].isin([10209010, 50106019])) &
                                           (headcount_data['Level_2'] != 50129026) &
                                           (headcount_data['Level_3'] != 50130297)][field_list]

        executive_base_df['Email 2'] = executive_base_df['Global ID'].apply(lambda x: str(int(x)) + '@grundfos.com')
        executive_base_df['Org unit'] = executive_base_df['Level_3_Name'].apply(
            lambda x: str(x)[0:str(x).find('(') - 1] if "(" in str(x)
            else ('The Poul Due Jensen Foundation Oper.' if str(x) == 'nan' else x))

        new_order = ['User/Employee ID', 'First Name', 'Last Name', 'Business Email Information Email Address',
                     'Org unit', 'Country/Region', 'Legal Company Legal Company Code', 'Global ID', 'Email 2']
        exec_df = executive_base_df[new_order]

        template_path = '03_TEMPLATE YTD Executive report.xlsx'
        o_name = 'Executive report'
        df_dict = {'Report': exec_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_apreg_newcomers_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1

        # Newcomers
        field_list = ['User/Employee ID', 'Formal Name', 'Employment Details Anniversary Date',
                      'Legal Company Legal Company Code', 'Legal Company Legal Company Name', 'Contract Type', 'FTE']
        cc_criteria = ['GAS', 'GTI', 'GIN', 'GIS', 'GPP', 'GJK', 'GNZ', 'GPA', 'GPK', 'GPM', 'GPV', 'GSI', 'GTH', 'GTS',
                       'GTW', 'GHK', 'GPC', 'GWC', 'GCH', 'GSH', 'GCQ']

        newcomers_df = headcount_data[((headcount_data['Legal Company Legal Company Code']).isin(cc_criteria)) &
                                      (headcount_data['Employment Details Anniversary Date'].dt.year == year) &
                                      (headcount_data['Employment Details Anniversary Date'].dt.month == month)][
            field_list]

        # Turnover
        to_field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                         'Employment Details Termination Date']

        turover_df = filter_turnover_dataframe_on_year_and_month(turnover_data, cc_criteria, to_field_list, year,
                                                                 month)

        df_dict = {'Leavers': turover_df, 'Newcomers': newcomers_df}
        template_path = '03_TEMPLATE APREG newcomers and leavers.xlsx'
        o_name = 'APREG newcomers and leavers'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gws_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Business Email Information Email Address', 'Global ID']
        legal_company_criteria = ['GWS']

        gws_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, legal_company_criteria, field_list)

        template_path = '03_TEMPLATE GWS Employees.xlsx'
        o_name = 'GWS Employee list'
        df_dict = {'Report': gws_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gms_gsse_fixed(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name',
                      'Manager User Sys ID', 'Direct Manager', 'Job Title', 'Contract Type', 'Contract End Date',
                      'Employee Status', 'Legal Company Legal Company Code', 'Legal Company Legal Company Name', 'FTE']
        cc_criteria = ['GMS', 'GSSE']

        gms_gsse_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, cc_criteria, field_list)
        gms_gsse_df = gms_gsse_df[gms_gsse_df['Contract Type'] == 'Fixed term']

        template_path = '03_TEMPLATE GMS_GSSE Employees.xlsx'
        o_name = 'GMS GSSE Fixed'
        df_dict = {'Report': gms_gsse_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gsse_cc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name']
        cc_criteria = ['GSSE']

        gsse_cc_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, cc_criteria, field_list)

        template_path = '03_TEMPLATE GSSE CC report.xlsx'
        o_name = 'GSSE CC list'
        df_dict = {'Report': gsse_cc_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_ops_white_newcomers_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1

        # Newcomers
        field_list = ['Level_4_Name', 'Level_4_Name', 'User/Employee ID', 'Formal Name', 'Subarea', 'FTE',
                      'Manager User Sys ID',
                      'Direct Manager', 'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
                      'Job Title', 'Employment Details Anniversary Date', 'Region 2']
        org_criteria = [50171611]

        ops_newcomers_df = headcount_data[((headcount_data['Level_3']).isin(org_criteria)) &
                                          (headcount_data['Subarea'] == 'White Collar') &
                                          (headcount_data['Employment Details Anniversary Date'].dt.year == year) &
                                          (headcount_data['Employment Details Anniversary Date'].dt.month == month)][
            field_list]

        # Turnover
        turnover_field_list = ['Level_4_Name', 'Level_4_Name', 'User/Employee ID', 'Formal Name', 'Subarea', 'FTE',
                               'Event Reason', 'Manager User Sys ID', 'Direct Manager',
                               'Legal Company Legal Company Code', 'Job Title',
                               'Job Role Job Role Name', 'Employment Details Termination Date', 'Region 2']

        ops_turnover_df = turnover_data[((turnover_data['Level_3']).isin(org_criteria)) &
                                        (turnover_data['Subarea'] == 'White Collar') &
                                        (turnover_data['Employment Details Termination Date'].dt.year == year) &
                                        (turnover_data['Employment Details Termination Date'].dt.month == month)][
            turnover_field_list]

        df_dict = {'Newcomers': ops_newcomers_df, 'Leavers': ops_turnover_df}
        template_path = '03_TEMPLATE Ops White Collar Newcomers and Leavers.xlsx'
        o_name = 'Ops White Collar Newcomers and Leavers'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gmh_cc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name']
        cc_criteria = ['GMH']

        gmh_cc_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, cc_criteria, field_list)

        df_dict = {'Report': gmh_cc_df}
        template_path = '03_TEMPLATE GMH CC report.xlsx'
        o_name = 'GMH CC report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_hvac_oem(self):
        headcount_data = self.dataprovider.get_headcount_data()
        org_criteria = 50172015
        field_list = ['User/Employee ID', 'Global ID', 'Formal Name', 'Group Band Code',
                      'Employment Details Anniversary Date',
                      'Location Location Name', 'Employee Group', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Level_5_Name', 'Subarea', 'Job Cluster Job Cluster Name',
                      'Job Family Name', 'Job Role Job Role Name', 'Manager User Sys ID', 'Direct Manager',
                      'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
                      'Cost Centre Cost Center Code',
                      'Cost Centre Cost Center Name', 'Job Title', 'Contract Type', 'FTE']
        hvac_df = headcount_data[headcount_data['Level_5'] == org_criteria][field_list]

        df_dict = {'Report': hvac_df}
        template_path = '03_TEMPLATE HVAC OEM HC Report.xlsx'
        o_name = 'HVAC OEM HC Report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_is_hc_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1

        field_list = ['User/Employee ID', 'Formal Name', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Country/Region',
                      'Direct Manager', 'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
                      'Group Band Code']
        org_criteria = 50171635

        is_hc_df = headcount_data[(headcount_data['Level_4'] == org_criteria)][field_list]

        to_field_list = ['User/Employee ID', 'Formal Name', 'Department Unit Department Unit Code',
                         'Department Unit Department Unit Name', 'Direct Manager', 'Legal Company Legal Company Code',
                         'Legal Company Legal Company Name', 'Group Band Code']

        is_turnover_df = turnover_data[(turnover_data['Level_4'] == org_criteria) &
                                       (turnover_data['Employment Details Termination Date'].dt.year == year) &
                                       (turnover_data['Employment Details Termination Date'].dt.month == month)][
            to_field_list]

        df_dict = {'HC': is_hc_df, 'Leavers': is_turnover_df}
        template_path = '03_TEMPLATE IS HC and Leavers.xlsx'
        o_name = 'IS HC and Leavers'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gsse_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Name', 'Location Location Name',
                      'Business Email Information Email Address', 'Level_3_Name', 'Level_4_Name',
                      'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                      'Manager User Sys ID', 'Direct Manager']
        lc_criteria = ['GSSE']
        gsse_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, lc_criteria, field_list)

        df_dict = {'Report': gsse_df}
        template_path = '03_TEMPLATE GSSE HC.xlsx'
        o_name = 'GSSE HC'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gms_flex_operators(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code', 'Job Role Job Role Name',
                      'Job Title', 'Disability Status']
        company_filter = ['GMS']

        gms_flex_operators_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter,
                                                                                 field_list)
        gms_flex_operators_df = gms_flex_operators_df[gms_flex_operators_df['Job Title'].str.contains('flex')]

        df_dict = {'Report': gms_flex_operators_df}
        template_path = '03_TEMPLATE GMS Flex Operators.xlsx'
        o_name = 'GMS Flex Operators'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gma_compensation_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        salary_data = self.dataprovider.get_salary_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Initials', 'Job Title', 'Subarea',
                      'Employment Details Anniversary Date', 'Cost Centre Cost Center Code',
                      'Cost Centre Cost Center Name', 'Legal Company Legal Company Code']
        legal_company_criteria = ['GMA']

        gma_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, legal_company_criteria, field_list)
        gma_df = gma_df.merge(salary_data[['User/Employee ID', 'Pay Component', 'Amount', 'Currency', 'Frequency']],
                              how='left',
                              on='User/Employee ID')

        df_dict = {'Report': gma_df}
        template_path = '03_TEMPLATE GMA Compensation Report.xlsx'
        o_name = 'GMA Compensation Report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_ppu_sti_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Legal Company Legal Company Name', 'Cost Centre Cost Center Code',
                      'Cost Centre Cost Center Name', 'STI Target %', 'STI Max %']
        legal_company_criteria = ['PPU']

        ppu_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, legal_company_criteria, field_list)

        df_dict = {'Report': ppu_df}
        template_path = '03_TEMPLATE PPU STI report.xlsx'
        o_name = 'PPU STI report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_ggd_gsa_geg_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code', 'Job Title',
                      'Job Role Job Role Name', 'Department Unit Department Unit Code',
                      'Manager User Sys ID', 'Direct Manager', 'Location Location Name', 'Cost Centre Cost Center Code',
                      'Group Band Code', 'IPE IPE Code']
        company_list = ['GGD', 'GSA', 'GEG', 'GDD']

        ggd_gsa_geg_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_list, field_list)

        df_dict = {'Report': ggd_gsa_geg_df}
        template_path = '03_TEMPLATE GGD GSA GEG Headcount.xlsx'
        o_name = 'GGD GSA GEG Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_global_service_delivery_newcomers_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1
        newcomer_field_list = ['User/Employee ID', 'Formal Name', 'Job Title', 'Job Role Job Role Name',
                               'Legal Company Legal Company Code', 'Manager User Sys ID', 'Direct Manager',
                               'Employment Details Anniversary Date']
        leaver_field_list = ['User/Employee ID', 'Formal Name', 'Job Title', 'Job Role Job Role Name',
                             'Legal Company Legal Company Code', 'Manager User Sys ID', 'Direct Manager',
                             'Employment Details Termination Date']

        sd_org = 50172416

        sd_newcomers_df = headcount_data[(headcount_data['Level_5'] == sd_org) &
                                         (headcount_data['Employment Details Anniversary Date'].dt.year == year) &
                                         (headcount_data['Employment Details Anniversary Date'].dt.month == month)][
            newcomer_field_list]

        sd_leavers_df = turnover_data[(turnover_data['Level_5'] == sd_org) &
                                      (turnover_data['Employment Details Termination Date'].dt.year == year) &
                                      (turnover_data['Employment Details Termination Date'].dt.month == month)][
            leaver_field_list]

        df_dict = {'Newcomers': sd_newcomers_df, 'Leavers': sd_leavers_df}
        template_path = '03_TEMPLATE Global Service Delivery Newcomers and Leavers.xlsx'
        o_name = 'Global Service Delivery Newcomers and Leavers'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gmh_ghu_gsse_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        company_filter = ['GMH', 'GHU', 'GSSE']
        field_list = ['User/Employee ID', 'Formal Name', 'Manager User Sys ID', 'Group Band Code', 'IPE IPE Code',
                      'Direct Manager', 'Legal Company Legal Company Code']
        df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, field_list)

        df_dict = {'Report': df}
        template_path = '03_TEMPLATE GMH GHU GSSE Headcount.xlsx'
        o_name = 'GMH GHU GSSE Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gms_cost_center_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'First Name', 'Last Name', 'Employee Group', 'Employee Status',
                      'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name', 'FTE']
        company_filter = ['GMS']

        df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, field_list)

        template_path = '03_TEMPLATE GMS Cost Center Report.xlsx'
        o_name = 'GMS Cost Center Report'
        df_dict = {'Report': df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gbh_gcr_ghu_gro_gse_gsl_gua_gbu_newcomers_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1
        company_filter = ['GBH', 'GCR', 'GHU', 'GRO', 'GSE', 'GSL', 'GUA', 'GBU']
        newcomers_field_list = ['Global ID', 'Formal Name', 'Employment Details Anniversary Date',
                                'Legal Company Legal Company Code', 'Job Title', 'Job Role Job Role Name',
                                'Contract Type', 'Subarea', 'Employee Sub Group Name', 'Group Band Code',
                                'Manager User Sys ID', 'Direct Manager', 'STI Target %', 'STI Max %',
                                'HR Business Partner Job Relationships User ID',
                                'HR Business Partner Job Relationships Name']

        df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, newcomers_field_list)

        leavers_field_list = ['Global ID', 'Formal Name', 'Employment Details Termination Date',
                              'Legal Company Legal Company Code', 'Job Title', 'Job Role Job Role Name',
                              'Contract Type', 'Subarea', 'Group Band Code', 'Manager User Sys ID', 'Direct Manager']

        t_df = turnover_data[(turnover_data['Legal Company Legal Company Code'].isin(company_filter)) &
                             (turnover_data['Employment Details Termination Date'].dt.year == year)][leavers_field_list]

        template_path = '03_TEMPLATE GBH GCR GHU GRO GSE GSL GUA GBU Newcomers and Turnover.xlsx'
        o_name = 'GBH GCR GHU GRO GSE GSL GUA GBU Newcomers and Turnover'
        df_dict = {'Newcomers': df, 'Leavers': t_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_bga_bgc_gbr_gco_gmx_gpe_newcomers_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1
        company_filter = ['BGA', 'BGC', 'GBR', 'GCO', 'GMX', 'GPE']
        newcomers_field_list = ['Global ID', 'Formal Name', 'Employment Details Anniversary Date',
                                'Legal Company Legal Company Code', 'Job Title', 'Job Role Job Role Name',
                                'Contract Type', 'Subarea', 'Employee Sub Group Name', 'Group Band Code',
                                'Manager User Sys ID', 'Direct Manager', 'STI Target %', 'STI Max %',
                                'HR Business Partner Job Relationships User ID',
                                'HR Business Partner Job Relationships Name']

        df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, newcomers_field_list)

        leavers_field_list = ['Global ID', 'Formal Name', 'Employment Details Termination Date',
                              'Legal Company Legal Company Code', 'Job Title', 'Job Role Job Role Name',
                              'Contract Type', 'Subarea', 'Group Band Code', 'Manager User Sys ID', 'Direct Manager']

        t_df = turnover_data[(turnover_data['Legal Company Legal Company Code'].isin(company_filter)) &
                             (turnover_data['Employment Details Termination Date'].dt.year == year)][leavers_field_list]

        template_path = '03_TEMPLATE BGA BGC GBR GCO GMX GPE Newcomers and Turnover.xlsx'
        o_name = 'BGA BGC GBR GCO GMX GPE Newcomers and Turnover'
        df_dict = {'Newcomers': df, 'Leavers': t_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_coo_monthly_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        fields = ['User/Employee ID', 'First Name', 'Last Name', 'Function Unit Function Unit Code',
                  'Function Unit Function Unit Name',
                  'Department Unit Department Unit Code', 'Department Unit Department Unit Name', 'Direct Subordinates',
                  'Job Title', 'Management Unit Management Unit Code', 'Management Unit Management Unit Name',
                  'Level_4', 'Level_4_Name', 'Level_5', 'Level_5_Name']

        coo_df = headcount_data[headcount_data['Management Unit Management Unit Name'] == 'Operations (COO)'][fields]

        df_dict = {'Report': coo_df}
        template_path = '03_TEMPLATE COO Monthly Headcount.xlsx'
        o_name = 'COO Monthly Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_ytd_turnover_wu_ind_dbs_cbs_aspac(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1
        region = 'APREG'
        t_fields = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                    'Department Unit Department Unit Name', 'Event Reason', 'Region 2']
        turnover_df = turnover_data[(turnover_data['Legal Company Legal Company Code'] != 'GIN') &
                                    (turnover_data['Employment Details Termination Date'].dt.year == year)][
            t_fields].copy()

        fields = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code', 'Level_4_Name',
                  'Department Unit Department Unit Name', 'Region 2']
        headcount_df = headcount_data[headcount_data['Legal Company Legal Company Code'] != 'GIN'][fields].copy()

        segments = ['WU', 'IND', 'DBS', 'CBS']
        headcount = [
            headcount_df[(headcount_df['Department Unit Department Unit Name'].str.contains(i)) & (
                    headcount_df['Region 2'] == region)][
                'User/Employee ID'].count() for i in segments]
        turnover = [
            turnover_df[
                (turnover_df['Department Unit Department Unit Name'].str.contains(i)) & (
                        turnover_df['Region 2'] == region)][
                'User/Employee ID'].count() for i in segments]

        df_dict = {'Segment': segments, 'Headcount': headcount, 'YTD Turnover': turnover}
        segment_turnover_df = pd.DataFrame(df_dict)
        segment_turnover_df['YTD Turnover %'] = segment_turnover_df['YTD Turnover'] / segment_turnover_df['Headcount']

        hc_tot = pd.DataFrame()
        to_tot = pd.DataFrame()
        for i in segments:
            df = headcount_df[
                (headcount_df['Department Unit Department Unit Name'].str.contains(i)) & (
                        headcount_df['Region 2'] == region)]
            hc_tot = hc_tot.append(df)
            to_df = turnover_df[
                (turnover_df['Department Unit Department Unit Name'].str.contains(i)) & (
                        turnover_df['Region 2'] == region)]
            to_tot = to_tot.append(to_df)

        template_path = '03_TEMPLATE YTD Turnover WU  IND  DBS CBS ASPAC.xlsx'
        o_name = 'YTD Turnover WU IND DBS CBS ASPAC'
        df_dict = {'Report': segment_turnover_df, 'Headcount': hc_tot, 'Turnover': to_tot}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_ytd_turnover_wu_ind_dbs_cbs_eewaa_wereg(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1
        ee = ['Hungary', 'Serbia', 'Croatia', 'Slovenia', 'Bulgaria', 'Romania', 'Czechia', 'Slovakia', 'Poland',
              'Ukraine',
              'Kazakhstan', 'Russia', 'Austria', 'Switzerland', 'Germany']

        we = ['Sweden', 'Norway', 'Denmark', 'Finland', 'Latvia', 'UK', 'Ireland', 'Belgium', 'Netherlands', 'France',
              'Spain',
              'Portugal', 'Italy', 'Greece']

        t_fields = ['User/Employee ID', 'Formal Name', 'Country/Region', 'Legal Company Legal Company Code',
                    'Level_4']
        turnover_df = turnover_data[(turnover_data['Employment Details Termination Date'].dt.year == year)][
            t_fields].copy()

        fields = ['User/Employee ID', 'Formal Name', 'Country/Region', 'Legal Company Legal Company Code',
                  'Department Unit Department Unit Name', 'Level_4_Name']
        headcount_df = headcount_data[fields].copy()

        eewaa_df = pd.DataFrame()
        wereg_df = pd.DataFrame()
        regions = {'ee': ee, 'we': we}
        regions_dict = {'ee': eewaa_df, 'we': wereg_df}

        segments = ['WU Sales', 'IND Sales', 'DBS Sales', 'CBS Sales']
        orgs_for_turnover = {'WU Sales': 50171653, 'IND Sales': 50171649, 'DBS Sales': 50171647, 'CBS Sales': 50171650}

        for region in regions:
            # Headcount
            headcount = [headcount_df[(headcount_df['Level_4_Name'] == i) & (
                headcount_df['Country/Region'].isin(regions[region]))]['User/Employee ID'].count() for i in segments]

            # Turnover
            turnover = [turnover_df[(turnover_df['Country/Region'].isin(regions[region]))
                                    & (turnover_df['Level_4'] == orgs_for_turnover[i])]['User/Employee ID'].count() for
                        i in segments]

            df_dict = {'Region': [region] * 4, 'Segments': segments, 'Headcount': headcount, 'YTD Turnover': turnover}
            df = pd.DataFrame(df_dict)
            df['YTD Turnover %'] = df['YTD Turnover'] / df['Headcount']
            regions_dict[region] = regions_dict[region].append(df)

            template_path = '03_TEMPLATE YTD Turnover WU  IND  DBS CBS EEWAA WEREG.xlsx'
            o_name = 'YTD Turnover WU  IND  DBS CBS EE WE'
            df_dict = {'EE': regions_dict['ee'], 'WE': regions_dict['we']}
            output_name = export_df(df_dict, template_path, o_name)
            return output_name

    @measure_running_time
    def rec_group_ehs_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        fields = ['User/Employee ID', 'Formal Name', 'Manager User Sys ID', 'Direct Manager', 'Job Role Job Role Name',
                  'Job Title', 'Group Band Code', 'Country/Region', 'Level_4_Name',
                  'Department Unit Department Unit Code', 'Department Unit Department Unit Name']

        df = headcount_data[headcount_data['Level_4_Name'] == 'Group EHS'][fields].copy()

        df_dict = {'Report': df}
        template_path = '03_TEMPLATE Group EHS Headcount.xlsx'
        o_name = 'Group EHS Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gno_turnover(self):
        turnover_data = self.dataprovider.get_turnover_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Initials', 'Business Email Information Email Address',
                      'Cell phone Formatted Phone Number', 'Job Title', 'Department Unit Department Unit Name',
                      'Location Location Name',
                      'Manager User Sys ID', 'Direct Manager', 'Cost Centre Cost Center Code',
                      'Employment Details Anniversary Date',
                      'Employment Details Termination Date', 'Date of Birth']
        company_filter = ['GNO']
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1

        df = filter_turnover_dataframe_on_year_and_month(turnover_data, company_filter, field_list, year, month)

        df_dict = {'Report': df}
        template_path = '03_TEMPLATE GNO Turnover.xlsx'
        o_name = 'GNO Turnover'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_cbs_cce_newcomers_and_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        prev_df = self.dataprovider.get_previous_month_headcount_data()
        lvl_5_org = 50172336
        year = date.today().year
        month = date.today().month

        nc_fields = ['User/Employee ID', 'Formal Name', 'Manager User Sys ID', 'Direct Manager',
                     'Employment Details Anniversary Date', 'Date of Birth']

        nc_df = headcount_data[(headcount_data['Level_5'] == lvl_5_org) &
                               ((headcount_data['Employment Details Anniversary Date'].dt.year == year))][
            nc_fields].copy()
        nc_df['Date of Birth'] = nc_df['Date of Birth'].apply(lambda x: x.strftime("%m-%d") if pd.notnull(x) else x)

        to_fields = ['User/Employee ID', 'Formal Name', 'Manager User Sys ID', 'Direct Manager',
                     'Employment Details Termination Date']
        to_df = turnover_data[(turnover_data['Level_5'] == lvl_5_org) &
                              ((turnover_data['Employment Details Termination Date'].dt.year == year))][
            to_fields].copy()

        df = extract_employee_data_changes(headcount_data, prev_df, ['Level_5', 'Level_5_Name'])
        movers = df[(df['Level_5 Change']) & (df['Curr_M_Level_5'] == lvl_5_org)]
        movers = movers.merge(headcount_data[['User/Employee ID', 'Date of Birth']], how='left', on='User/Employee ID')
        movers['Date of Birth'] = movers['Date of Birth'].apply(lambda x: x.strftime("%m-%d") if pd.notnull(x) else x)

        df_dict = {'Newcomers': nc_df, 'Leavers': to_df, 'Movers': movers}
        path = '03_TEMPLATE CBS CEE Newcomers and Leavers.xlsx'
        o_name = 'CBS CEE Newcomers and Leavers'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_metteam_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        prev_df = self.dataprovider.get_previous_month_headcount_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1

            fields = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                      'Department Unit Department Unit Short Name']

        curr_df = headcount_data[fields].copy()
        curr_df.columns = [x + " - Current" if x not in ['User/Employee ID', 'Formal Name'] else x for x in
                           curr_df.columns]
        prev_data = prev_df[fields].copy()
        prev_data.columns = [x + " - Previous" if x not in ['User/Employee ID', 'Formal Name'] else x for x in
                             prev_data.columns]

        compare = curr_df.merge(prev_data, how='outer', on=['User/Employee ID', 'Formal Name'])
        compare.fillna('None', inplace=True)

        compare['Changed Org Unit?'] = compare['Department Unit Department Unit Code - Current'] == compare[
            'Department Unit Department Unit Code - Previous']
        compare['Changed Org Unit?'] = compare['Changed Org Unit?'].apply(
            lambda x: 'Same org unit' if x else 'Changed org unit')

        compare['Changed Department?'] = compare['Department Unit Department Unit Short Name - Current'] == compare[
            'Department Unit Department Unit Short Name - Previous']
        compare['Changed Department?'] = compare['Changed Department?'].apply(
            lambda x: 'Same department' if x else 'Changed department')

        terminated_employees = list(turnover_data['User/Employee ID'])
        compare['Active/Terminated'] = compare['User/Employee ID'].apply(
            lambda x: "Terminated" if x in terminated_employees else 'Active')

        newcomers = list(headcount_data[(headcount_data['Employment Details Original Start Date'].dt.year == year) &
                                        (headcount_data['Employment Details Original Start Date'].dt.month == month)][
                             'User/Employee ID'])
        compare['New Hire'] = compare['User/Employee ID'].apply(
            lambda x: "New hire" if x in newcomers else 'Not new hire')
        compare = compare[['User/Employee ID', 'Formal Name',
                           'Legal Company Legal Company Code - Current',
                           'Department Unit Department Unit Code - Current',
                           'Department Unit Department Unit Name - Current',
                           'Department Unit Department Unit Short Name - Current',
                           'Department Unit Department Unit Code - Previous',
                           'Department Unit Department Unit Name - Previous',
                           'Department Unit Department Unit Short Name - Previous',
                           'Changed Org Unit?', 'Changed Department?', 'Active/Terminated',
                           'New Hire']]

        df_dict = {'Report': compare}
        path = '03_TEMPLATE MetTeam Report.xlsx'
        o_name = 'MetTeam Report'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_opss_americas(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        manager_criteria = [32225, 77980, 79088, 59055, 74768, 30070, 25413, 58745, 59437, 79093,
                            76628, 76888, 58021, 24105, 59875, 62286, 58079, 74223, 72762, 74172,
                            79561, 29333, 24785, 75229, 79726, 3521, 63006, 69857, 79594, 3271,
                            29024, 57854, 24114, 28104, 74070, 69810, 3191, 76431]
        org_criteria = 50171652
        fields = ['User/Employee ID', 'Formal Name', 'Gender', 'Cost Centre Cost Center Code',
                  'Cost Centre Cost Center Name', 'Legal Company Legal Company Code',
                  'Legal Company Legal Company Name', 'Manager User Sys ID', 'Direct Manager',
                  'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                  'FTE', 'Country Code', 'Country/Region', 'Direct Subordinates', 'Employee Status',
                  'Is People Manager Y/N']

        base_df = headcount_data[(headcount_data['Manager User Sys ID'].isin(manager_criteria)) &
                                 (headcount_data['Level_4'] == org_criteria)][fields]

        # Output 1
        am_headcount_per_company_df = pd.DataFrame(base_df.groupby(
            ['Legal Company Legal Company Code', 'Legal Company Legal Company Name', 'Cost Centre Cost Center Code',
             'Cost Centre Cost Center Name']).size()).reset_index().rename({0: 'Employee count'}, axis=1)

        # Output 2
        service_area_lookup_path = 'ReportingSourceCode/data/static/Service area lookup.xlsx'
        open_positions_path = 'ReportingSourceCode/data/static/01_Source_SF.xlsx'
        if "Tests" in os.getcwd():
            service_area_lookup_path = 'C:/Users/78595/PycharmProjects/recurring_reports/ReportingSourceCode/data/static/Service area lookup.xlsx'
            open_positions_path = 'C:/Users/78595/PycharmProjects/recurring_reports/ReportingSourceCode/data/static/01_Source_SF.xlsx'

        sf_op_df = pd.read_excel(open_positions_path)

        americas_op_df = sf_op_df[(sf_op_df['Hiring Manager User Sys ID']).isin(manager_criteria)]

        # Output 3.1

        em_count_df = pd.crosstab(base_df['Gender'], [base_df['Employee Status'], base_df['Country/Region']],
                                  margins=True)
        norm_em_count_df = pd.crosstab(base_df['Gender'], [base_df['Employee Status'], base_df['Country/Region']],
                                       normalize='columns')
        em_summary_df = em_count_df.append(norm_em_count_df)

        # Output 3.2
        def soc_cat(soc):
            if soc < 3:
                return "< 3"
            else:
                return "> 3"

        merged_df_2 = base_df.copy()
        merged_df_2['SoC'] = base_df['Direct Subordinates'].apply(soc_cat)
        merged_df_2 = merged_df_2[merged_df_2['Direct Subordinates'] > 0]

        manager_count_df = pd.crosstab([merged_df_2['Gender'], merged_df_2['SoC']], [merged_df_2['Country/Region']],
                                       margins='columns')
        if '< 3' not in merged_df_2['SoC'].value_counts().index.tolist():
            content = [0] * len(manager_count_df.columns)
            manager_count_df.loc['< 3'] = content

        norm_manager_count_df = pd.crosstab([merged_df_2['Gender'], merged_df_2['SoC']],
                                            [merged_df_2['Country/Region']], normalize='columns')
        man_summary_df = manager_count_df.append(norm_manager_count_df)

        # Output 3.3
        op_positions_per_region_df = pd.crosstab(americas_op_df['Country'], americas_op_df['Hire Reason'])
        op_positions_per_region_df = op_positions_per_region_df.reset_index()

        # Output 4
        manager_data = get_manager_data(headcount_data, ['Legal Company Legal Company Code'])

        span_of_control_base = base_df.merge(manager_data, how='left', on='Manager User Sys ID')

        span_of_control = pd.crosstab(
            [span_of_control_base['Manager User Sys ID'], span_of_control_base['Direct Manager'],
             span_of_control_base['Manager Legal Company Legal Company Code'],
             span_of_control_base['Employee Status']], span_of_control_base['Employee Status']).reset_index()

        span_of_control.rename({'Active': 'Span of Control'}, axis=1, inplace=True)
        span_of_control_final = span_of_control[
            ['Manager User Sys ID', 'Direct Manager', 'Manager Legal Company Legal Company Code', 'Span of Control']]

        # Output 5
        soc_sum = pd.crosstab(span_of_control['Span of Control'], span_of_control['Employee Status'])
        soc_sum = soc_sum.reset_index().rename({'Active': 'Manager Count'}, axis=1)

        # Output 6
        year = date.today().year
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code', 'Country/Region',
                      'Subarea',
                      'Department Unit Department Unit Name', 'Employment Details Original Start Date']
        newcomers_df = headcount_data[((headcount_data['Manager User Sys ID']).isin(manager_criteria)) & (
                    headcount_data['Employment Details Original Start Date'].dt.year == year)][field_list].copy()

        # Output 7
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code', 'Subarea',
                      'Department Unit Department Unit Name', 'Employment Details Termination Date']
        turnover_df = turnover_data[((turnover_data['Manager User Sys ID']).isin(manager_criteria)) & (
                    turnover_data['Employment Details Termination Date'].dt.year == year)][field_list].copy()

        df_dict = {'Headcounts': am_headcount_per_company_df, 'Open Positions': americas_op_df,
                   'Employees per Country': em_summary_df, 'Managers per Country': man_summary_df,
                   'Positions per Country': op_positions_per_region_df, 'SoC per Manager': am_headcount_per_company_df,
                   'Newcomers': newcomers_df, 'Leavers': turnover_df}

        template_path = '03_TEMPLATE Open Positions and HC in Americas Service and Solutions.xlsx'
        o_name = 'Open Positions and HC in Americas Service and Solutions'
        output_name = export_df_with_header_and_index(df_dict, template_path, o_name)
        return output_name


