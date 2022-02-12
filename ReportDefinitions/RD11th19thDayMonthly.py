from datetime import date
import pandas as pd
import numpy as np
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
import datetime
from ReportingSourceCode.SupportFunctions.extract_employee_data_changes import extract_employee_data_changes


class RD11th19thDayMonthly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_is_newcomers_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        prev_headcount = self.dataprovider.get_previous_month_headcount_data()
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1

        org_unit = 50171635

        field_list = ['User/Employee ID', 'Formal Name', 'Job Role Job Role Name',
                      'Employment Details Anniversary Date', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Manager User Sys ID', 'Direct Manager']

        is_newcomers_df = headcount_data[(headcount_data['Level_4'] == org_unit) &
                                         (headcount_data['Employment Details Anniversary Date'].dt.year == year) &
                                         (headcount_data['Employment Details Anniversary Date'].dt.month == month)][
            field_list]

        field_list = ['User/Employee ID', 'Formal Name', 'Job Role Job Role Name',
                      'Employment Details Termination Date', 'Manager User Sys ID', 'Direct Manager',
                      'Department Unit Department Unit Name']

        is_turnover_df = turnover_data[((turnover_data['Level_4']) == org_unit) &
                                       (turnover_data['Employment Details Termination Date'].dt.year == year) &
                                       (turnover_data['Employment Details Termination Date'].dt.month == month)][
            field_list]

        # Create check table to see who moved to or from IS
        movement_field_list = ['User/Employee ID', 'Level_4', 'Level_4_Name']
        expanded_field_list = ['User/Employee ID', 'Formal Name', 'Job Role Job Role Name',
                               'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                               'Manager User Sys ID', 'Direct Manager', 'Level_4', 'Level_4_Name']

        is_movement_check_df = prev_headcount[movement_field_list].copy()
        is_movement_check_df = is_movement_check_df.merge(headcount_data[expanded_field_list], how='left',
                                                          on='User/Employee ID')
        is_movement_check_df = is_movement_check_df.rename(
            {'Level_4_x': 'PM_Level_4', 'Level_4_Name_x': 'PM_Level_4_Name',
             'Level_4_y': 'AM_Level_4', 'Level_4_Name_y': 'AM_Level_4_Name'}, axis=1)
        is_movement_check_df.dropna(axis=0, subset=['PM_Level_4', 'AM_Level_4_Name'], inplace=True)

        is_movement_check_df['Movement_From_IS'] = (is_movement_check_df['PM_Level_4_Name'] != is_movement_check_df[
            'AM_Level_4_Name']) & (is_movement_check_df['PM_Level_4'] == org_unit)
        is_movement_check_df['Movement_To_IS'] = (is_movement_check_df['PM_Level_4_Name'] != is_movement_check_df[
            'AM_Level_4_Name']) & (is_movement_check_df['AM_Level_4'] == org_unit)

        is_movement_check_df = is_movement_check_df[
            (is_movement_check_df['Movement_From_IS'] == True) | (is_movement_check_df['Movement_To_IS'] == True)]

        is_movement_check_df = is_movement_check_df[
            ['User/Employee ID', 'Formal Name', 'Job Role Job Role Name',
             'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
             'Manager User Sys ID', 'Direct Manager', 'PM_Level_4', 'PM_Level_4_Name', 'AM_Level_4', 'AM_Level_4_Name',
             'Movement_From_IS', 'Movement_To_IS']]

        # Create check table to see who changed department in IS
        movement_field_list = ['User/Employee ID', 'Department Unit Department Unit Code',
                               'Department Unit Department Unit Name']
        expanded_field_list = ['User/Employee ID', 'Formal Name', 'Job Role Job Role Name',
                               'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                               'Manager User Sys ID', 'Direct Manager', 'Level_4', 'Level_4_Name']

        is_department_change_df = prev_headcount[prev_headcount['Level_4'] == org_unit][movement_field_list].copy()
        is_department_change_df = is_department_change_df.merge(headcount_data[expanded_field_list], how='left',
                                                                on='User/Employee ID')

        is_department_change_df = is_department_change_df.rename(
            {'Department Unit Department Unit Code_x': 'PM Nearest Node',
             'Department Unit Department Unit Name_x': 'PM Nearest Node name',
             'Department Unit Department Unit Code_y': 'AM Nearest Node',
             'Department Unit Department Unit Name_y': 'AM Nearest Node name'}, axis=1)

        is_department_change_df.dropna(axis=0, subset=['PM Nearest Node', 'AM Nearest Node'], inplace=True)

        is_department_change_df['Department change'] = (
                is_department_change_df['PM Nearest Node name'] != is_department_change_df['AM Nearest Node name'])
        is_department_change_df = is_department_change_df[is_department_change_df['Department change'] == True]
        is_department_change_df = is_department_change_df[
            ['User/Employee ID', 'Formal Name', 'Job Role Job Role Name', 'Manager User Sys ID', 'Direct Manager',
             'Level_4', 'Level_4_Name', 'PM Nearest Node', 'PM Nearest Node name', 'AM Nearest Node',
             'AM Nearest Node name']]

        template_path = '03_TEMPLATE IS Newcomers and Leavers.xlsx'
        o_name = 'IS Newcomers and Leavers'
        df_dict = {'Newcomers': is_newcomers_df, 'Leavers': is_turnover_df, 'Movement': is_movement_check_df,
                   'Department change': is_department_change_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_hr_newcomers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        org_criteria = ['HR (CHRO)']
        field_list = ['User/Employee ID', 'Formal Name', 'Level_3_Name', 'Department Unit Department Unit Name',
                      'Business Email Information Email Address',
                      'Employment Details Anniversary Date', 'Manager User Sys ID', 'Direct Manager',
                      'Job Role Job Role Name', 'Job Title', 'Country/Region']
        departments_to_exclude = [50192975, 50173075, 50192664, 50173076, 50191980]
        job_roles_to_exclude = ['Apprentice/Trainee(Associate Prof.)', 'Canteen Staff', 'Canteen Staff Skilled',
                                'Cook Skilled', 'Driver', 'Graduate (Associate Specialist)']
        positions_to_exclude = ['Breakfast Service', 'Captain']

        hr_newcomers_base_df = headcount_data[(headcount_data['Business Email Information Email Address'] != '#') &
                                              (~headcount_data['Job Role Job Role Name'].isin(job_roles_to_exclude)) &
                                              (~headcount_data['Job Title'].isin(positions_to_exclude))][field_list]

        chro_newcomers_df = hr_newcomers_base_df[(hr_newcomers_base_df['Level_3_Name']).isin(org_criteria) &
                                                 ~(hr_newcomers_base_df['Department Unit Department Unit Name']).isin(
                                                     departments_to_exclude)]

        non_chro_hr_role_df = hr_newcomers_base_df[~(hr_newcomers_base_df['Level_3_Name']).isin(org_criteria) &
                                                   (hr_newcomers_base_df['Job Role Job Role Name'].str.contains('HR'))]

        non_chro_hr_position_df = hr_newcomers_base_df[~(hr_newcomers_base_df['Level_3_Name']).isin(org_criteria) &
                                                       (hr_newcomers_base_df['Job Title'].str.contains('HR'))]

        df_dict = {'CHRO Newcomers': chro_newcomers_df, 'HR Job Role': non_chro_hr_role_df,
                   'HR Position': non_chro_hr_position_df}
        template_path = '03_TEMPLATE HR Newcomers.xlsx'
        o_name = 'HR Newcomers'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_younited(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Date of Birth', 'Department Unit Department Unit Name',
                      'Legal Company Legal Company Code', 'Employee Status', 'Job Title']
        company_filter = ['GSSE']

        gsse_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, field_list)
        gsse_df['B-month'] = gsse_df['Date of Birth'].dt.month
        gsse_df['B-day'] = gsse_df['Date of Birth'].dt.day
        younited_df = gsse_df[['User/Employee ID', 'Formal Name', 'B-day', 'B-month',
                               'Department Unit Department Unit Name',
                               'Legal Company Legal Company Code', 'Employee Status', 'Job Title']].copy()

        df_dict = {'Report': younited_df}
        template_path = '03_TEMPLATE You-Nited report.xlsx'
        o_name = 'You-Nited report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gwc_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        company_criteria = ['GWC']
        filed_list = ['User/Employee ID', 'Formal Name', 'Department Unit Department Unit Name', 'Group Band Code',
                      'Initials', 'Job Role Job Role Name', 'Manager User Sys ID', 'Direct Manager', 'Job Title',
                      'Contract Type', 'FTE']

        gwc_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_criteria, filed_list)

        df_dict = {'Report': gwc_df}
        template_path = '03_TEMPLATE GWC Headcount.xlsx'
        o_name = 'GWC Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_doa_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Initials', 'Formal Name', 'Location Location Name',
                      'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
                      'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name', 'Group Band Code',
                      'Business Email Information Email Address', 'Organizational Level',
                      'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                      'Level_4_Name', 'Level_5_Name', 'Level_6_Name', 'Level_7_Name', 'Level_8_Name', 'Level_9_Name',
                      'Level_10_Name', 'Level_11_Name', 'Employment Details Anniversary Date', 'Manager User Sys ID',
                      'Direct Manager', 'Manager Legal Company Legal Company Code', 'Job Title',
                      'Job Cluster Job Cluster Name', 'Job Family Name', 'Job Role Job Role Name']
        company_filter = ['GSH', 'GCH', 'GCQ', 'GHK', 'GPC', 'GWC', 'GTH', 'GPP', 'GIS', 'GPV', 'GJK', 'GPK',
                          'GSI', 'SSG', 'GPM', 'GAS', 'GTI', 'GTW', 'GSI', 'GPA', 'GNZ']

        manager_cc_df = get_manager_data(headcount_data, ['Legal Company Legal Company Code'])
        headcount_data2 = headcount_data.merge(manager_cc_df, how='left', on='Manager User Sys ID')
        doa_lc_flat_df = filter_dataframe_on_legal_company_and_field_name(headcount_data2, company_filter, field_list)

        org_criteria_lvl_4 = [50171662, 50171650, 50171626]
        doa_org_flat_df = headcount_data2[(headcount_data2['Level_4']).isin(org_criteria_lvl_4)][field_list].copy()

        df_dict = {'Legal Company Report': doa_lc_flat_df, 'Organizational Report': doa_org_flat_df}
        template_path = '03_TEMPLATE DoA Report.xlsx'
        o_name = 'DoA Report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_wereg_all_segment_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Initials', 'Formal Name', 'Location Location Name',
                      'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
                      'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name', 'Group Band Code',
                      'Business Email Information Email Address', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Level_4_Name', 'Level_5_Name', 'Level_6_Name',
                      'Level_7_Name', 'Level_8_Name', 'Level_9_Name', 'Level_10_Name', 'Level_11_Name',
                      'Employment Details Anniversary Date', 'Manager User Sys ID', 'Direct Manager',
                      'Manager Legal Company Legal Company Code', 'Job Title', 'Job Cluster Job Cluster Name',
                      'Job Family Name', 'Job Role Job Role Name', 'Organizational Level']
        cc_criteria = ['BGE', 'BGP', 'GB', 'GBJ', 'GBL', 'GBW', 'GDK', 'GEF', 'GFD', 'GFI', 'GHA', 'GHO',
                       'GIT', 'GLA', 'GLL', 'GMA', 'GNL', 'GNO', 'GOE', 'GPH', 'GPI', 'GSF', 'GSV', 'PGF',
                       'WGB']

        manager_cc_df = get_manager_data(headcount_data, ['Legal Company Legal Company Code'])
        headcount_data2 = headcount_data.merge(manager_cc_df, how='left', on='Manager User Sys ID')
        wereg_lc_flat_df = filter_dataframe_on_legal_company_and_field_name(headcount_data2, cc_criteria, field_list)

        org_criteria_lvl_4 = [50171631, 50171627, 50171652, 50171647, 50171649, 50171660, 50171664, 50171657, 50171662,
                              50171628, 50171650, 50171653, 50171626, 50135106]
        wereg_org_flat_df = headcount_data2[(headcount_data2['Level_4']).isin(org_criteria_lvl_4)][field_list].copy()

        df_dict = {'Legal Company Report': wereg_lc_flat_df, 'Organizational Report': wereg_org_flat_df}
        template_path = '03_TEMPLATE WEREG All Segment Report.xlsx'
        o_name = 'WEREG All Segment Report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gb_1_4_dk_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        fields = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code', 'Group Band Code',
                  'Employee Sub Group Name']
        companies = ['GBJ', 'GBB', 'GDK', 'GMA', 'GOE', 'PDJ']

        dk_df = headcount_data[(headcount_data['Legal Company Legal Company Code'].isin(companies)) &
                               (headcount_data['Group Band Code'].isin(['01', '02', '03', '04']))][fields]

        df_dict = {'Report': dk_df}
        template_path = '03_TEMPLATE GB 1-4 DK Headcount.xlsx'
        o_name = 'GB 1-4 DK Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gwc_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        company_criteria = ['GWC']
        filed_list = ['User/Employee ID', 'Formal Name', 'Department Unit Department Unit Code', 'Group Band Code',
                      'Initials', 'Job Role Job Role Name', 'Manager User Sys ID', 'Direct Manager', 'Job Title',
                      'Contract Type', 'FTE']

        gwc_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_criteria, filed_list)

        df_dict = {'Report': gwc_df}
        template_path = '03_TEMPLATE GWC Headcount.xlsx'
        o_name = 'GWC Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_service_2_reports(self):
        headcount_data = self.dataprovider.get_headcount_data()
        a_year = date.today().year
        a_month = date.today().month
        p_year = date.today().year
        p_month = date.today().month - 1

        if a_month == 1:
            p_year = a_year - 1
            p_month = 12

        upper_limit = datetime.datetime(a_year, a_month, 15)
        lower_limit = datetime.datetime(p_year, p_month, 15)

        field_list = ['User/Employee ID', 'Formal Name', 'Job Title', 'Legal Company Legal Company Code',
                      'Legal Company Legal Company Name', 'Business Email Information Email Address', 'Country Code',
                      'Country/Region', 'Employment Details Anniversary Date', 'Region 2']
        lvl_4_org_criteria = 50171652

        ss_df = headcount_data[headcount_data['Level_4'] == lvl_4_org_criteria]
        ss_newcomers_df = ss_df[(ss_df['Employment Details Anniversary Date'] >= lower_limit) &
                                (ss_df['Employment Details Anniversary Date'] <= upper_limit)][field_list].copy()

        hc_field_list = ['User/Employee ID', 'Formal Name', 'Department Unit Department Unit Name', 'Job Title',
                         'Job Role Job Role Name', 'Business Email Information Email Address',
                         'Legal Company Legal Company Code', 'Legal Company Legal Company Name', 'Country Code',
                         'Country/Region', 'Region 2']

        ss_hc_df = ss_df[hc_field_list].copy()

        df_dict = {'1 Newcomers': ss_newcomers_df, '2 Total HC': ss_hc_df}
        template_path = '03_TEMPLATE Grundfos Service & Solutions.xlsx'
        o_name = 'Grundfos Service & Solutions'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gb_gpi_wgb_gcb_gpu_ppu_mxp_gca_newcomers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        companies = ['GB', 'GPI', 'WGB', 'GCB', 'GPU', 'PPU', 'MXP', 'GCA']
        fields = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code', 'Manager User Sys ID',
                  'Direct Manager', 'Employment Details Anniversary Date']
        year = date.today().year

        df = headcount_data[(headcount_data['Legal Company Legal Company Code'].isin(companies)) &
                            (headcount_data['Employment Details Anniversary Date'].dt.year == year)][fields]

        df_dict = {'Report': df}
        template_path = '03_TEMPLATE GB GPI WGB GCB GPU PPU MXP GCA - Newcomers.xlsx'
        o_name = 'GB GPI WGB GCB GPU PPU MXP GCA - Newcomers'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_headcount_for_dashboard(self):
        expanded_data = self.dataprovider.get_expanded_ec_data()
        fields = ['Global ID', 'User/Employee ID', 'Formal Name', 'Employee Status', 'Legal Company Legal Company Code',
                  'Level_3_Name', 'Level_4_Name', 'Contract Type', 'FTE']

        dashboard_df = expanded_data[fields]

        df_dict = {'Report': dashboard_df}
        template_path = '03_TEMPLATE Headcount for Dashboard.xlsx'
        o_name = 'Headcount for Dashboard'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_ggp_pug_tracking_reports(self):
        expanded_data = self.dataprovider.get_expanded_ec_data()
        headcount_data = self.dataprovider.get_headcount_data()
        previous_data = self.dataprovider.get_previous_month_headcount_data()
        ggp_users = [3040, 3246, 45745, 48756, 67199, 48778, 48861, 49118, 35928, 49186, 51316, 51410, 78521, 51304,
                     51348, 68790, 51134, 51097, 50199, 52209, 53700, 51101, 53717, 52658, 55898, 55907, 55955, 56467,
                     58460, 58463, 54816, 54874, 58458, 60802, 60803, 60810, 60807, 60807, 72308, 51239, 75542, 74959,
                     75591, 69998, 74203, 74214, 74216, 74227, 74226, 74205, 74309, 74240, 74206, 3704, 20290, 20462,
                     20531, 20797, 22772, 27004, 31192, 35375, 36215, 53211, 70247, 70248, 70249, 78493]
        pug_users = [45774, 48944, 54016, 53760, 25018, 34227, 62124, 56700, 24914, 56993, 58896, 38931, 59963, 57187,
                     36416, 7076, 57778, 56449, 54325, 3109, 51668, 42144, 34987, 27004, 55340, 55307, 50924, 27779,
                     25571, 51134, 51058, 52852, 25536, 35753, 46985, 22179, 56467, 47114, 58463, 26290, 51092, 51409,
                     59711, 23135, 33114, 41655, 70594, 56869, 56222, 38444, 58338, 56968, 54177, 41615, 28993, 48778,
                     45584, 49424, 100139, 52363, 58746, 56944, 42597, 49027, 68610, 70168, 51325, 71166, 64195, 61568,
                     48756, 37796, 37622, 71107, 71931, 50736, 31787, 49950, 46739, 59117, 58626, 35153, 23253, 53385,
                     76145, 59823, 46713, 78258, 46964, 64141, 74770, 45549, 68553, 55098, 69403, 55898, 30111, 49118,
                     63144, 46989, 51811, 46582, 68611, 25615, 59872, 70318, 51097, 46986, 46310, 56936, 58611]
        groups_to_loop = {'GGP': ggp_users, 'PUG': pug_users}

        for group in groups_to_loop:
            fields = ['User/Employee ID', 'Formal Name', 'Job Title', 'Employee Status']
            summary_df = expanded_data[expanded_data['User/Employee ID'].isin(groups_to_loop[group])][
                fields].drop_duplicates()
            summary_df.loc[summary_df['Employee Status'] == 'Terminated', 'Job Title'] = 'Terminated'

            users_from_summary_df = list(summary_df['User/Employee ID'])
            missing_users = list(set(groups_to_loop[group]) - set(users_from_summary_df))
            if len(missing_users) > 0:
                missing_user_data_dict = {'User/Employee ID': missing_users,
                                          'Job Title': ['Terminated'] * len(missing_users)}
                missing_user_df = pd.DataFrame(missing_user_data_dict)
                summary_df = summary_df.append(missing_user_df)
            summary_df = summary_df[['User/Employee ID', 'Formal Name', 'Job Title']]

            fields_to_compare = ['User/Employee ID', 'Formal Name', 'Department Unit Department Unit Code',
                                 'Department Unit Department Unit Name', 'Job Title', 'Job Role Job Role Name',
                                 'Group Band Code']

            actual_month = headcount_data[headcount_data['User/Employee ID'].isin(groups_to_loop[group])][
                fields_to_compare]
            actual_month.columns = ['Actual ' + x if x not in ['User/Employee ID', 'Formal Name'] else x for x in
                                    actual_month.columns]

            previous_month = previous_data[previous_data['User/Employee ID'].isin(groups_to_loop[group])][
                fields_to_compare]
            previous_month.columns = ['Previous ' + x if x not in ['User/Employee ID', 'Formal Name'] else x for x in
                                      previous_month.columns]
            compare_df = previous_month.merge(actual_month, how='left', on=['User/Employee ID', 'Formal Name'])

            compare_df.fillna('None', inplace=True)
            compare_df['Change Department?'] = compare_df.apply(
                lambda x: "Changed Department" if x['Previous Department Unit Department Unit Name'] != x[
                    'Actual Department Unit Department Unit Name'] else "Same Department", axis=1)
            compare_df['Change position?'] = compare_df.apply(
                lambda x: "Changed Position" if x['Previous Job Title'] != x['Actual Job Title'] else "Same Position",
                axis=1)
            compare_df['Change job role?'] = compare_df.apply(
                lambda x: "Changed Job Role" if x['Previous Job Role Job Role Name'] != x[
                    'Actual Job Role Job Role Name'] else "Same Job Role", axis=1)
            compare_df['Change band level?'] = compare_df.apply(
                lambda x: "Changed Band" if x['Previous Group Band Code'] != x[
                    'Actual Group Band Code'] else "Same Band", axis=1)
            compare_df['Change Status?'] = (compare_df['Change Department?'] == "Changed Department") | (
                    compare_df['Change position?'] == "Changed Position") | (
                                                   compare_df['Change job role?'] == "Changed Job Role") | (
                                                   compare_df['Change band level?'] == "Changed Band")
            compare_df['Change Status?'] = compare_df['Change Status?'].apply(
                lambda x: "Change" if x == True else "No Change")

            actual_employees = list(actual_month['User/Employee ID'])
            previuos_employees = list(previous_month['User/Employee ID'])
            terminated_employees = [x for x in previuos_employees if x not in actual_employees]
            compare_df.loc[compare_df['User/Employee ID'].isin(terminated_employees), 'Change Status?'] = 'Terminated'

            no_change = compare_df[compare_df['Change Status?'] == 'No Change']['User/Employee ID'].count()
            dep_change = compare_df[compare_df['Change Department?'] == 'Changed Departmen']['User/Employee ID'].count()
            pos_change = compare_df[compare_df['Change position?'] == 'Changed Position']['User/Employee ID'].count()
            jr_change = compare_df[compare_df['Change job role?'] == 'Changed Job Role']['User/Employee ID'].count()
            gb_change = compare_df[compare_df['Change band level?'] == 'Changed Band']['User/Employee ID'].count()

            types = ['No Change', 'Changed Job Role', 'Changed Band', 'Changed Position', 'Changed Department']
            values = [no_change, jr_change, gb_change, pos_change, dep_change]
            df_dict = {'Change Type': types, 'Number of Employees': values}
            chart_df = pd.DataFrame(df_dict)

            df_dict = {'Overview': summary_df, 'Chart': chart_df, 'Compare': compare_df}
            template_path = '03_TEMPLATE GGP & PUG Tracking Reports.xlsx'
            o_name = f'{group} Tracking Report'
            export_df(df_dict, template_path, o_name)

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
    def travel_report(self):
        it17_path = 'C:/Users/78595/PycharmProjects/recurring_reports/ReportingSourceCode/data/dynamic/travel.txt'
        headcount_data = self.dataprovider.get_headcount_data()

        it17_df = pd.read_table(it17_path, header=2)
        it17_df['End year'] = it17_df['End Date'].apply(lambda x: str(x)[-4:])

        it17_df = it17_df[it17_df['End year'] == '9999'][
            ['Pers.No.', 'Reimbursement Group for Meals/', 'Start Date', 'End Date']]

        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Legal Company Legal Company Name', 'Group Band Code']

        travel_df = headcount_data[field_list].copy()
        travel_df = travel_df.merge(it17_df[['Pers.No.', 'Reimbursement Group for Meals/']], how='left',
                                    left_on='User/Employee ID', right_on='Pers.No.')
        travel_df = travel_df[['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                               'Legal Company Legal Company Name', 'Group Band Code', 'Reimbursement Group for Meals/']]

        df_dict = {'Report': travel_df}
        template_path = '03_TEMPLATE Travel report.xlsx'
        o_name = 'IT17 Travel report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def dk_payroll(self):
        headcount_data = self.dataprovider.get_headcount_data()

        sap_path = 'C:/Users/78595/PycharmProjects/recurring_reports/ReportingSourceCode/data/dynamic/Worksheet in ALV.xls'

        sap_input = pd.ExcelFile(sap_path)
        payroll_sap_df = pd.read_excel(sap_input, 'Format', header=0)

        # DK payroll check
        company_list = ['GMA', 'GBJ', 'GOE', 'PDJ', 'GDK', 'GBB', 'STX']
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Legal Company Legal Company Name', 'Pay Group Pay Group Code', 'Pay Group Pay Group Name',
                      'Employee Sub Group Code', 'Employee Sub Group Name', 'Cost Centre Cost Center Code']
        base_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_list, field_list)

        # 1 check
        round_1_payroll_area = ['M1', 'M2', 'M3']
        round_1_lc = ['GBB', 'GBJ', 'GDK', 'GMA', 'GOE', 'PDJ']
        merged_r1_df = base_df.copy()
        merged_r1_df.loc[((merged_r1_df['Legal Company Legal Company Code']).isin(round_1_lc)) &
                         ~((merged_r1_df['Pay Group Pay Group Code']).isin(round_1_payroll_area)), 'Round 1'] = 'Error'
        error_output_1 = merged_r1_df[merged_r1_df['Round 1'] == 'Error']

        # 2 check
        merged_r2_df = base_df[~base_df['Cost Centre Cost Center Code'].isna()].copy()
        merged_r2_df['Round 2'] = np.nan
        cc_code = 'Cost Centre Cost Center Code'
        lc_code = 'Legal Company Legal Company Code'

        merged_r2_df.loc[
            ((merged_r2_df[lc_code] == 'GBJ') & ~(merged_r2_df[cc_code].str.endswith('66'))) |
            ((merged_r2_df[lc_code] == 'GMA') & ~(merged_r2_df[cc_code].str.endswith('60'))) |
            ((merged_r2_df[lc_code] == 'GDK') & ~(merged_r2_df[cc_code].str.endswith('56'))) |
            ((merged_r2_df[lc_code] == 'GOE') & ~(merged_r2_df[cc_code].str.endswith('96B'))) |
            ((merged_r2_df[lc_code] == 'GBB') & ~(merged_r2_df[cc_code].str.endswith('02C'))) |
            ((merged_r2_df[lc_code] == 'GLL') & ~(merged_r2_df[cc_code].str.endswith('42B'))) |
            ((merged_r2_df[lc_code] == 'PDJ') & ~(merged_r2_df[cc_code].str.endswith('97A'))), 'Round 2'] = 'Error 2'

        error_output_2 = merged_r2_df[merged_r2_df['Round 2'] == 'Error 2']

        # 3 check
        m1 = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27]
        m2 = [40, 41, 42, 43, 46, 47, 48, 49, 50]
        m3 = [60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 73, 74, 75, 76, 77, 80, 90]
        pra = 'Pay Group Pay Group Code'
        esg_c = 'Employee Sub Group Code'

        merged_r3_df = base_df.copy()
        merged_r3_df['Round 3'] = np.nan
        merged_r3_df.loc[(merged_r3_df[pra] == 'M1') & ~((merged_r3_df[esg_c]).isin(m1)) |
                         (merged_r3_df[pra] == 'M2') & ~((merged_r3_df[esg_c]).isin(m2)) |
                         (merged_r3_df[pra] == 'M3') & ~((merged_r3_df[esg_c]).isin(m3)), 'Round 3'] = 'Error 3'
        error_output_3 = merged_r3_df[merged_r3_df['Round 3'] == 'Error 3']

        fields4 = ['User/Employee ID', 'Displayed First Name', 'Last Name', 'Country/Region', 'Legal Company',
                   'Cost Centre (Cost Center Code)', 'Cost Centre (Label)', 'Pay Component']
        fields5 = ['User/Employee ID', 'Displayed First Name', 'Last Name', 'Country/Region', 'Legal Company',
                   'Cost Centre (Cost Center Code)', 'Cost Centre (Label)', 'National ID']
        df = self.hrbp_df[self.hrbp_df['Country/Region'] == 'Denmark'].copy()

        # 4 check
        error_output_4 = df[df['Pay Component'] == '9001-Base salary'][fields4]
        error_output_4.drop_duplicates(inplace=True)

        # 5 check
        error_output_5 = df[(df['National ID'].isna()) | df['Cost Centre (Cost Center Code)'].isna()][fields5]
        error_output_5.drop_duplicates(inplace=True)

        df_dict = {'Round 1': error_output_1, 'Round 3': error_output_3,
                   'Round 4': error_output_4, 'Round 5': error_output_5}
        template_path = '03_TEMPLATE DK Payroll Check.xlsx'
        o_name = 'DK Payroll Check'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name


