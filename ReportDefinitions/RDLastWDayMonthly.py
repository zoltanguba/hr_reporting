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


class RDLastWDayMonthly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_dk_white_collar_newcomers_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        company_filter = ['GBB', 'GBJ', 'GDK', 'GFI', 'GLL', 'GMA', 'GOE', 'PDJ', 'SEN', 'STX']
        country_filter = ['Denmark']
        termination_fields = ['User/Employee ID', 'Formal Name', 'Employment Details Termination Date',
                              'Department Unit Department Unit Code', 'Legal Company Legal Company Code',
                              'Legal Company Legal Company Name', 'Reason for leaving']
        newcomers_fields = ['User/Employee ID', 'Formal Name', 'Employment Details Anniversary Date',
                            'Legal Company Legal Company Code', 'Legal Company Legal Company Name', 'Job Title',
                            'Department Unit Department Unit Code', 'Is People Manager Y/N', 'Group Band Code',
                            'Contract Type', 'FTE']
        year = date.today().year
        month = date.today().month
        next_year = date.today().year
        next_month = month + 1
        if month == 12:
            next_year = year + 1
            next_month = 1

        newcomers_actual_df = headcount_data[(headcount_data['Country/Region'].isin(country_filter)) &
                                             (headcount_data['Subarea'] == 'White Collar') &
                                             (headcount_data['Employment Details Anniversary Date'].dt.year == year) &
                                             (headcount_data['Employment Details Anniversary Date'].dt.month == month)][
            newcomers_fields]
        newcomers_next_df = headcount_data[(headcount_data['Country/Region'].isin(country_filter)) &
                                           (headcount_data['Subarea'] == 'White Collar') &
                                           (headcount_data[
                                                'Employment Details Anniversary Date'].dt.year == next_year) &
                                           (headcount_data[
                                                'Employment Details Anniversary Date'].dt.month == next_month)][
            newcomers_fields]
        turnover_df = turnover_data[(turnover_data['Country/Region'].isin(country_filter)) &
                                    (turnover_data['Subarea'] == 'White Collar') &
                                    (turnover_data['Employment Details Termination Date'].dt.year == year) &
                                    (turnover_data['Employment Details Termination Date'].dt.month == month)][
            termination_fields]

        template_path = '03_TEMPLATE DK White Collar Newcomers and Leavers.xlsx'
        o_name = 'DK White Collar Newcomers and Leavers'
        df_dict = {'Leavers': turnover_df, 'Newcomers Actual': newcomers_actual_df,
                   'Newcomers Next Month': newcomers_next_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_stx_external_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Legal Company Legal Company Name']
        stx_df = headcount_data[(headcount_data['Legal Company Legal Company Code'] == 'STX') &
                                (headcount_data['Employee Group'] == '9-External')][field_list]

        template_path = '03_TEMPLATE STX External Headcount.xlsx'
        o_name = 'STX External Headcount'
        df_dict = {'Report': stx_df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_legal_company_summary_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        lcc_summary_df = pd.DataFrame(
            headcount_data.groupby('Legal Company Legal Company Code').size()).reset_index().rename(
            {0: 'Total Headcount'}, axis=1)

        df_dict = {'Report': lcc_summary_df}
        template_path = '03_TEMPLATE Legal company summary.xlsx'
        o_name = 'Legal company summary'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_opss(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()

        year = date.today().year
        month = date.today().month
        next_year = date.today().year
        next_month = month + 1
        if month == 12:
            next_year = year + 1
            next_month = 1

        service_area_lookup_path = 'ReportingSourceCode/data/static/Service area lookup.xlsx'
        open_positions_path = 'ReportingSourceCode/data/static/01_Source_SF.xlsx'
        if "Tests" in os.getcwd():
            service_area_lookup_path = 'C:/Users/78595/PycharmProjects/recurring_reports/ReportingSourceCode/data/static/Service area lookup.xlsx'
            open_positions_path = 'C:/Users/78595/PycharmProjects/recurring_reports/ReportingSourceCode/data/static/01_Source_SF.xlsx'

        region_data = self.dataprovider.get_region_data()
        ss_subareas_df = pd.read_excel(service_area_lookup_path)
        ss_subareas_df['Region'] = ss_subareas_df['Region'].str.upper()

        field_list = ['User/Employee ID', 'Formal Name', 'Gender', 'Cost Centre Cost Center Code',
                      'Cost Centre Cost Center Name', 'Legal Company Legal Company Code',
                      'Legal Company Legal Company Name', 'Manager User Sys ID', 'Direct Manager', 'Level_4_Name',
                      'Level_5_Name', 'Country/Region', 'Region 2', 'Employee Status', 'Direct Subordinates']
        org_unit_criteria = [50171652]

        ss_df = headcount_data[(headcount_data['Level_4']).isin(org_unit_criteria)][field_list]
        merged_df = ss_df.merge(ss_subareas_df[['Legal Company Legal Company Code', 'Area']], how='left',
                                on='Legal Company Legal Company Code')

        # Replacing the region with 'Global' where needed
        merged_df.loc[(merged_df['Level_5_Name'] == 'Global Service Functions') |
                      (merged_df['Level_5_Name'] == 'Customer Backend Support & Service Excel'),
                      'Region 2'] = 'Global'
        merged_df[['Area']] = merged_df[['Area']].fillna('No Area')
        merged_df[['Region 2']] = merged_df[['Region 2']].fillna('No Region')

        # Output 1: Headcount tab
        headcount_per_company_df = pd.DataFrame(merged_df.groupby(
            ['Legal Company Legal Company Code', 'Legal Company Legal Company Name', 'Cost Centre Cost Center Code',
             'Cost Centre Cost Center Name', 'Area']).size()).reset_index().rename({0: 'Employee count'}, axis=1)

        # Output 2: Open Positions tab
        LOOKUP_region2 = region_data[['2-Digit country code', 'Region 2']].drop_duplicates()
        global_hir_mngs = [['Torben', 'Tommy Due', 'Anne Marie'], ['Hadberg', 'Høy', 'Hansen']]
        sf_op_df = pd.read_excel(open_positions_path)
        sf_op_df = sf_op_df.merge(LOOKUP_region2[['2-Digit country code', 'Region 2']], how='left', left_on='Country',
                                  right_on='2-Digit country code')
        sf_op_df.drop(['2-Digit country code'], axis=1, inplace=True)

        # Changing region where it should be 'Global'
        sf_op_df.loc[(sf_op_df['Hiring Manager First Name'].isin(global_hir_mngs[0])) &
                     (sf_op_df['Hiring Manager Last Name'].isin(global_hir_mngs[1])), 'Region 2'] = 'Global'

        sf_op_df['Legal Company Legal Company Code'] = sf_op_df['Function Unit'].apply(
            lambda x: x[x.rfind('(') + 1:x.rfind(')')])

        # match area names to regions
        sf_op_df = sf_op_df.merge(ss_subareas_df[['Area', 'Legal Company Legal Company Code']], how='left',
                                  on='Legal Company Legal Company Code')
        sf_op_df[['Area']] = sf_op_df[['Area']].fillna('No Area')
        sf_op_df[['Region 2']] = sf_op_df[['Region 2']].fillna('No Region')

        # Output 3: Span of Control per Manager and Company tab
        soc_per_manager_and_company = pd.crosstab([merged_df['Manager User Sys ID'], merged_df['Direct Manager'],
                                                   merged_df['Legal Company Legal Company Code']],
                                                  merged_df['Employee Status']).reset_index()

        # Output 4: Span of Control tab
        soc = pd.crosstab([merged_df['Manager User Sys ID']], merged_df['Employee Status']).reset_index()
        soc_df = soc['Active'].value_counts().reset_index().rename(
            {'index': 'Span of Control', 'Active': 'Manager Count'}, axis=1)

        # 5.1 Total Employee Count
        em_count_df = pd.crosstab(merged_df['Gender'], merged_df['Employee Status'])
        norm_em_count_df = pd.crosstab(merged_df['Gender'], merged_df['Employee Status'], normalize=True)
        em_summary_df = pd.concat([em_count_df, norm_em_count_df], axis=1)
        em_summary_df = em_summary_df.reset_index()

        # 5.2 Total Manager Count
        def soc_cat(soc):
            if soc < 3:
                return "< 3"
            elif soc >= 3 and soc < 6:
                return "3 <= < 6"
            elif soc >= 6:
                return "> 6"
            else:
                return 0

        merged_df_2 = merged_df.copy()
        merged_df_2['SoC'] = merged_df_2['Direct Subordinates'].apply(soc_cat)
        merged_df_2 = merged_df_2[merged_df_2['Direct Subordinates'] > 0]

        manager_count_df = pd.crosstab(merged_df_2['Gender'], merged_df_2['SoC'], margins=True)[['< 3', 'All']]
        norm_manager_count_df = pd.crosstab(merged_df_2['Gender'], merged_df_2['SoC'], margins=True, normalize=True)[
            ['< 3', 'All']]

        manager_sum_df = pd.concat([manager_count_df, norm_manager_count_df], axis=1)
        manager_sum_df = manager_sum_df.reset_index()

        # 5.3 Employees per Region
        emp_per_region_df = pd.crosstab(merged_df['Region 2'], merged_df['Employee Status'])
        emp_per_region_df = emp_per_region_df.reset_index()

        # 5.3 Open Positions per Region
        op_positions_per_region_df = pd.crosstab(sf_op_df['Region 2'], sf_op_df['Hire Reason'])
        op_positions_per_region_df = op_positions_per_region_df.reset_index()

        # 5.4 Regional Summary
        df_filt = merged_df_2[merged_df_2['SoC'].isin(['3 <= < 6', '> 6'])]
        soc_cross = pd.crosstab(df_filt['Region 2'], [df_filt['Employee Status'], df_filt['SoC'],
                                                      df_filt['Gender']]).transpose().reset_index()
        soc_cross['Employee Status'] = 'Managers count'
        emp_cross = pd.crosstab(merged_df['Region 2'],
                                [merged_df['Employee Status'], merged_df['Gender']]).transpose().reset_index()
        emp_cross['Employee Status'] = 'Employee count'
        man_cross = pd.crosstab(merged_df_2['Region 2'],
                                [merged_df_2['Employee Status'], merged_df_2['Gender']]).transpose().reset_index()
        man_cross['Employee Status'] = 'Managers count'
        regional_summary_df = pd.concat([soc_cross, emp_cross, man_cross], sort=False)

        # 5.4 Turnover Summary
        hc = headcount_data[['User/Employee ID', 'Level_4_Name', 'Employee Status']].copy()
        hc.loc[hc['Level_4_Name'] != 'Service & Solutions', 'Level_4_Name'] = 'Group'
        group_cross = pd.crosstab(hc['Level_4_Name'], hc['Employee Status'])
        to = turnover_data[(turnover_data['Employment Details Termination Date'].dt.year == year) &
                           (turnover_data['Employment Details Termination Date'].dt.month == month)][
            ['User/Employee ID', 'Level_4_Name', 'Employee Status']].copy()
        to.loc[to['Level_4_Name'] != 'Service & Solutions', 'Level_4_Name'] = 'Group'
        group_to = pd.crosstab(to['Level_4_Name'], to['Employee Status'])
        group_hc_to_cross = pd.concat([group_cross, group_to], axis=1)
        group_hc_to_cross = group_hc_to_cross.reset_index()

        # 5.5 Area and Region Summary
        area_and_region_summary_df = pd.crosstab(merged_df['Area'], merged_df['Region 2'])
        area_and_region_summary_df = area_and_region_summary_df.reset_index()

        # 5.6 Vacancies by Area
        op_positions_per_region_and_area_df = pd.crosstab([sf_op_df['Region 2'], sf_op_df['Area']],
                                                          sf_op_df['Hire Reason'])
        op_positions_per_region_and_area_df = op_positions_per_region_and_area_df.reset_index()

        df_dict = {'Headcounts': headcount_per_company_df, 'Open Positions': sf_op_df,
                   'Span of Control by Manager': soc_per_manager_and_company,
                   'Span of Control': soc_df, 'Employee Summary': em_summary_df,
                   'Manager Summary': manager_sum_df, 'Employees per Region': emp_per_region_df,
                   'Regional Open Positions': op_positions_per_region_df,
                   'Regional Summary': regional_summary_df, 'Turnover Summary': group_hc_to_cross,
                   'Area and Region Summary': area_and_region_summary_df,
                   'Open Positions per Region': op_positions_per_region_and_area_df}

        template_path = '03_TEMPLATE Service and Solutions.xlsx'
        o_name = 'Open Positions and HC in Service and Solutions'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_dk_cost_centers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['Global ID', 'User/Employee ID', 'Formal Name', 'Group Band Code',
                      'Legal Company Legal Company Code', 'Initials',
                      'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name']
        company_filter = ['GMA', 'GOE', 'GDK', 'GBJ']

        dk_cost_center_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, field_list)

        df_dict = {'Report': dk_cost_center_df}
        template_path = '03_TEMPLATE DK Cost Centers.xlsx'
        o_name = 'DK Cost Centers'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_americas_industry_heacdount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        today = date.today()
        field_list = ['User/Employee ID', 'Formal Name', 'Organisational Company Code',
                      'Legal Company Legal Company Name', 'Gender', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name',
                      'Subarea', 'Location Location Name', 'Job Role Job Role Name', 'Cost Centre Cost Center Code',
                      'Cost Centre Cost Center Name', 'Job Title', 'Group Band Code', 'IPE IPE Code',
                      'Manager User Sys ID',
                      'Direct Manager', 'Employment Details Anniversary Date', 'Is People Manager Y/N', 'STI Target %',
                      'Seniority in Years']
        lvl_5_org_filter = 50172241

        ind_am_df = headcount_data[headcount_data['Level_5'] == lvl_5_org_filter][field_list]

        df_dict = {'Report': ind_am_df}
        template_path = '03_TEMPLATE Americas Industry Headcount.xlsx'
        o_name = 'Americas Industry Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_global_monthy_turnover(self):
        turnover_data = self.dataprovider.get_turnover_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Event Reason', 'Reason for leaving',
                      'Employment Details Termination Date', 'Legal Company Legal Company Code']
        year = date.today().year

        turnover_df = turnover_data[turnover_data['Employment Details Termination Date'].dt.year == year][
            field_list].copy()

        df_dict = {'Report': turnover_df}
        template_path = '03_TEMPLATE Global Monthly Turnover.xlsx'
        o_name = 'Global Monthly Turnover'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gza_cost_center_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        fields = ['User/Employee ID', 'Formal Name', 'Cost Centre Cost Center Code',
                  'Cost Centre Cost Center Name', 'Legal Company Legal Company Code']
        company = ['GZA']
        df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company, fields)

        df_dict = {'Report': df}
        template_path = '03_TEMPLATE GZA Cost Center Report.xlsx'
        o_name = 'GZA Cost Center Report'
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
    def rec_dk_death_in_service(self):
        turnover_data = self.dataprovider.get_turnover_data()
        fields = ['User/Employee ID', 'Formal Name', 'Manager User Sys ID', 'Direct Manager',
                  'Department Unit Department Unit Name',
                  'Legal Company Legal Company Code', 'Country/Region', 'Employment Details Termination Date',
                  'Event Reason']
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1

        df = turnover_data[(turnover_data['Event Reason'] == '2012-Death in service') &
                           (turnover_data['Country/Region'] == 'Denmark') &
                           (turnover_data['Employment Details Termination Date'].dt.year == year)][fields]

        df_dict = {'Report': df}
        path = '03_TEMPLATE DK Death in Service Report.xlsx'
        o_name = 'DK Death in Service Report'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_ind_service_sales_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Organisational Company Code',
                      'Legal Company Legal Company Name', 'Gender', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name',
                      'Subarea', 'Location Location Name', 'Job Role Job Role Name', 'Cost Centre Cost Center Code',
                      'Cost Centre Cost Center Name', 'Job Title', 'Group Band Code', 'IPE IPE Code',
                      'Manager User Sys ID',
                      'Direct Manager', 'Employment Details Anniversary Date', 'Is People Manager Y/N', 'STI Target %',
                      'Seniority in Years']
        lvl_5_org_filter = 50172418

        ind_am_df = headcount_data[headcount_data['Level_5'] == lvl_5_org_filter][field_list]

        df_dict = {'Report': ind_am_df}
        template_path = '03_TEMPLATE IND Service Sales Report.xlsx'
        o_name = 'IND Service Sales Report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name






