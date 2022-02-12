from ReportingSourceCode.DataProvider.IDataProvider import IDataProvider
from ReportingSourceCode.DataLoader.DataLoader import DataLoader


class DataProvider(IDataProvider):

    def __init__(self, data: DataLoader):
        super().__init__(data)

    def get_region_data(self):
        return self.data.recurring_region_df.copy()

    def get_expanded_ec_data(self):
        return self.data.expanded_ec_df.copy()

    def get_headcount_data(self):
        return self.data.ec_headcount_df.copy()

    def get_turnover_data(self):
        return self.data.ec_turnover_df.copy()

    def get_salary_data(self):
        return self.data.ec_salary_df.copy()

    def get_rehire_data(self):
        return self.data.ec_rehire_df.copy()

    def get_flat_file(self):
        return self.data.ec_flat_file.copy()

    def get_manager_line_data(self):
        return self.data.manager_line_df.copy()

    def get_previous_month_headcount_data(self):
        return self.data.previous_month_ec_headcount_df.copy()



