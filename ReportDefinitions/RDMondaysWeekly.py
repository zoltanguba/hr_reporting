from datetime import date

from ReportingSourceCode.ReportDefinitions.IReportDefinition import IReportDefinition
from ReportingSourceCode.DataProvider.IDataProvider import IDataProvider
from ReportingSourceCode.SupportFunctions.export_dataframes_to_excel import (export_df,
                                                                             export_df_with_header,
                                                                             export_df_with_header_and_index)
from ReportingSourceCode.SupportFunctions.filter_dataframes import filter_dataframe_on_legal_company_and_field_name
from ReportingSourceCode.SupportFunctions.decorators import measure_running_time
from ReportingSourceCode.SupportFunctions.get_manager_data import get_manager_data
import os


class RDMondaysWeekly(IReportDefinition):

    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_gms_weekly_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Employee Status', 'Legal Company Legal Company Code',
                      'Gender', 'Contract Type', 'Contract End Date', 'Employment Details Anniversary Date',
                      'Job Title', 'Department Unit Department Unit Name', 'Subarea', 'Level_3_Name', 'Group Band Code',
                      'IPE IPE Code', 'Job Role Job Role Name', 'Manager User Sys ID', 'Direct Manager',
                      'Disability Status', 'Cell phone Formatted Phone Number']
        company_filter = ['GMS']
        gms_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, field_list)

        df_dict = {'Report': gms_df}
        path = '03_TEMPLATE GMS weekly headcount.xlsx'
        o_name = 'GMS Weekly Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gmh_weekly_turnover(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Employment Details Termination Date', 'Event Reason',
                      'Manager User Sys ID', 'Direct Manager']
        company_filter = ['GMH']

        managers_manager = get_manager_data(headcount_data, ['Manager User Sys ID', 'Direct Manager'])

        gmh_weekly_turnover_df = turnover_data[(turnover_data['Legal Company Legal Company Code'])
            .isin(company_filter)][field_list]
        gmh_weekly_turnover_df = gmh_weekly_turnover_df.merge(managers_manager, how='left', on='Manager User Sys ID')

        df_dict = {'Report': gmh_weekly_turnover_df}
        path = '03_TEMPLATE GMH Weekly Turnover.xlsx'
        o_name = 'GMH Weekly Turnover'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_ytd_hungary_turnover(self):
        turnover_data = self.dataprovider.get_turnover_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Employment Details Termination Date',
                      'Department Unit Department Unit Name', 'Legal Company Legal Company Code',
                      'Job Title', 'Job Role Job Role Name', 'Manager User Sys ID', 'Direct Manager']

        legal_company_criteria = ['GMH', 'GHU', 'GSSE']

        turover_df = turnover_data[turnover_data['Legal Company Legal Company Code'].isin(legal_company_criteria)][field_list]

        df_dict = {'Report': turover_df}
        path = '03_TEMPLATE YTD Hungary Turnover.xlsx'
        o_name = 'YTD Hungary Turnover'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_new_managers_and_promotions(self):
        already_seen_managers_test_file = 'ReportingSourceCode/ReportDefinitions/temporary_files/temp_manager_id_list.txt'
        # Check if the method is being called through a Unittest - if so, the working directory is different and the
        # file path needs to be changed
        if "Tests" in os.getcwd():
            already_seen_managers_test_file = 'C:/Users/78595/PycharmProjects/recurring_reports/ReportingSourceCode/ReportDefinitions/temporary_files/temp_manager_id_list.txt'

        # Loading in the Manager IDs from the previous report
        previous_manager_ids_list = []

        try:
            with open(already_seen_managers_test_file, 'r') as reader:
                for line in reader:
                    manager_from_previous = line[:-1]
                    previous_manager_ids_list.append(int(manager_from_previous[:-2]))
        except FileNotFoundError:
            print("No existing Temporary Manager ID file found, no filter is going to be applied ")

        headcount_data = self.dataprovider.get_headcount_data()
        previous_headcount_data = self.dataprovider.get_previous_month_headcount_data()

        # Generate manager data
        manager_data_df = get_manager_data(headcount_data, ['Business Email Information Email Address'])

        field_list = ['User/Employee ID', 'Formal Name', 'Business Email Information Email Address',
                      'Job Role Job Role Name', 'Job Title', 'Country/Region', 'Legal Company Legal Company Code',
                      'Level_3_Name', 'Department Unit Department Unit Name', 'Location Location Name',
                      'Group Band Code', 'Is People Manager Y/N', 'Employment Details Anniversary Date',
                      'Manager User Sys ID', 'Direct Manager', 'Position Entry Date']
        year = date.today().year

        # Create newcomer manager list: Is People Manager Y/N == 'Yes' and Seniority Year == 2020
        newcomer_managers_df = headcount_data[(headcount_data['Employment Details Anniversary Date'].dt.year == year) &
                                              (headcount_data['Is People Manager Y/N'] == 'Yes')][field_list].copy()
        newcomer_managers_df = newcomer_managers_df.merge(manager_data_df, how='left', on='Manager User Sys ID')
        newcomer_managers_df['Type'] = 'Newcomer manager'

        # Check the employees who went from Is People Manager Y/N == 'No' to Is People Manager Y/N == 'Yes'
        changes_prev_df = previous_headcount_data[['User/Employee ID', 'Formal Name', 'Is People Manager Y/N']].copy()
        changes_prev_df.dropna(axis=0, inplace=True)
        changes_prev_df = changes_prev_df.rename({'Is People Manager Y/N': 'Prev Is People Manager Y/N'}, axis=1)
        field_list.remove('Formal Name')
        changes_compare_df = changes_prev_df.merge(headcount_data[field_list],
                                                   how='left',
                                                   on='User/Employee ID')
        changes_compare_df['Promoted to Manager'] = changes_compare_df['Prev Is People Manager Y/N'] != changes_compare_df['Is People Manager Y/N']

        managerial_promotions_df = changes_compare_df[(~(changes_compare_df['Is People Manager Y/N']).isna()) &
                                                      (changes_compare_df['Promoted to Manager'] == True) &
                                                      (changes_compare_df['Is People Manager Y/N'] == 'Yes')][
            field_list].copy()
        managerial_promotions_df = managerial_promotions_df.merge(manager_data_df, how='left', on='Manager User Sys ID')
        managerial_promotions_df['Type'] = 'Promoted manager'

        # Append promotion data to newcomer managers data
        combined_new_manager_df = newcomer_managers_df.append(managerial_promotions_df, sort=False)

        combined_new_manager_df = combined_new_manager_df[
            ~(combined_new_manager_df['User/Employee ID']).isin(previous_manager_ids_list)]

        # Save the list of the Manager IDs present in the latest generated report
        temp_manager_id_list = list(combined_new_manager_df['User/Employee ID'])
        with open(already_seen_managers_test_file, 'a+') as writer:
            for manager_id in temp_manager_id_list:
                writer.write("{}\n".format(manager_id))

        df_dict = {'Data': combined_new_manager_df}
        path = '03_TEMPLATE Newcomer Managers and Promotions.xlsx'
        o_name = 'Newcomer Managers and Promotions'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_ytd_global_leavers(self):
        year = date.today().year
        turnover_data = self.dataprovider.get_turnover_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Employment Details Termination Date', 'Manager User Sys ID',
                      'Direct Manager', 'Legal Company Legal Company Code', 'Legal Company Legal Company Name']

        ytd_leavers_df = turnover_data[turnover_data['Employment Details Termination Date'].dt.year == year][field_list]
        ytd_leavers_df['Type of action'] = 'Leaver'

        df_dict = {'Report': ytd_leavers_df}
        path = '03_TEMPLATE YTD Leavers Report.xlsx'
        o_name = 'YTD Leavers Report'
        output_name = export_df(df_dict, path, o_name)
        return output_name






