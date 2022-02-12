from datetime import date
from ReportingSourceCode.ReportDefinitions.IReportDefinition import IReportDefinition
from ReportingSourceCode.DataProvider.IDataProvider import IDataProvider
from ReportingSourceCode.SupportFunctions.export_dataframes_to_excel import (export_df,
                                                                             export_df_with_header,
                                                                             export_df_with_header_and_index)
from ReportingSourceCode.SupportFunctions.filter_dataframes import filter_dataframe_on_legal_company_and_field_name
from ReportingSourceCode.SupportFunctions.decorators import measure_running_time


class RDThursdaysWeekly(IReportDefinition):

    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def weekly_dk_turnover(self) -> str:
        turnover_data = self.dataprovider.get_turnover_data()
        company_filter = ['GBB', 'GBJ', 'GDK', 'GFI', 'GLL', 'GMA', 'GOE', 'PDJ', 'SEN', 'STX']
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Employment Details Termination Date', 'Manager User Sys ID', 'Direct Manager']

        gmh_weekly_turnover_df = filter_dataframe_on_legal_company_and_field_name(turnover_data,
                                                                                  company_filter, field_list)

        df_dict = {'Report': gmh_weekly_turnover_df}
        path = '03_TEMPLATE Weekly DK Leavers.xlsx'
        o_name = 'Weekly DK Leavers'
        output_name = export_df(df_dict, path, o_name)

        return output_name

    @ measure_running_time
    def rec_americas_region_hc(self) -> str:
        region_data = self.dataprovider.get_region_data()
        headcount_data = self.dataprovider.get_headcount_data()

        americas_regions = region_data[region_data['Region'] == 'AMERICAS'][
            ['2-Digit country code', 'Legal Company code', 'Region']].rename({'2-Digit country code': 'Country',
                                                                              'Legal Company code': 'Legal Company Legal Company Code'},
                                                                             axis=1).drop_duplicates()

        field_list = ['User/Employee ID', 'Formal Name', 'Level_3_Name', 'Level_4_Name',
                      'Business Email Information Email Address', 'Legal Company Legal Company Code',
                      'Organisational Company Code', 'Country/Region', 'Region']

        americas_region_df = headcount_data.merge(americas_regions[['Legal Company Legal Company Code', 'Region']],
                                                                   how='left',
                                                                   on='Legal Company Legal Company Code')[field_list]
        americas_region_df = americas_region_df[americas_region_df['Region'] == 'AMERICAS']

        df_dict = {'Report': americas_region_df}
        path = '03_TEMPLATE Americas Region Headcount.xlsx'
        o_name = 'Americas Region Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_szekesfehervar_turnover(self) -> str:
        turnover_data = self.dataprovider.get_turnover_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code', 'Manager User Sys ID',
                      'Direct Manager', 'Employment Details Termination Date', 'Event Reason', 'Reason for leaving',
                      'Location Location Name']
        site_filter = 'HU-Székesfehérvár'

        szkf_turnover_df = turnover_data[turnover_data['Location Location Name'] == site_filter][field_list].copy()

        df_dict = {'Report': szkf_turnover_df}
        path = '03_TEMPLATE Székesfehérvár terminations.xlsx'
        o_name = 'Székesfehérvár terminations'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_global_1office_newcomers_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()

        newcomer_field_list = ['User/Employee ID', 'Formal Name', 'Code', 'Job Title', 'Group Band Code',
                               'Employment Details Anniversary Date', 'Manager User Sys ID', 'Direct Manager',
                               'Department Unit Department Unit Name', 'Legal Company Legal Company Code',
                               'Country/Region', 'Region 2']
        org_criteria = 50171648

        year = date.today().year

        g1o_nc_df = headcount_data[(headcount_data['Function Unit Function Unit Code'] == org_criteria) &
                                   (headcount_data['Employment Details Anniversary Date'].dt.year == year)][
            newcomer_field_list]

        leaver_field_list = ['User/Employee ID', 'Formal Name', 'Job Title', 'Group Band Code',
                             'Employment Details Termination Date', 'Manager User Sys ID', 'Direct Manager',
                             'Department Unit Department Unit Name', 'Legal Company Legal Company Code',
                             'Country/Region', 'Region 2']

        g1o_lv_df = turnover_data[(turnover_data['Function Unit Function Unit Code'] == org_criteria) &
                                  (turnover_data['Employment Details Termination Date'].dt.year == year)][
            leaver_field_list]

        path = '03_TEMPLATE Global 1Office Newcomers and Leavers.xlsx'
        o_name = 'Global 1Office Newcomers and Leavers'
        df_dict = {'Newcomers': g1o_nc_df, 'Leavers': g1o_lv_df}
        output_name = export_df(df_dict, path, o_name)
        return output_name







