from abc import ABCMeta, abstractmethod
from ReportingSourceCode.DataLoader.DataLoader import DataLoader


class IDataProvider(metaclass=ABCMeta):

    def __init__(self, data: DataLoader):
        if isinstance(data, DataLoader):
            self.data = data
        else:
            raise TypeError("Invalid Dataloader object provided.")

    @abstractmethod
    def get_region_data(self):
        """Method to get region lookup data"""

    @abstractmethod
    def get_expanded_ec_data(self):
        """Method to get cleaned and extended EC data"""

    @abstractmethod
    def get_headcount_data(self):
        """Method to get headcount data"""

    @abstractmethod
    def get_turnover_data(self):
        """Method to get turnover data"""

    @abstractmethod
    def get_salary_data(self):
        """Method to get salary data"""

    @abstractmethod
    def get_rehire_data(self):
        """Method to get rehire data"""

    @abstractmethod
    def get_flat_file(self):
        """Method to get the flat org structure"""

    @abstractmethod
    def get_manager_line_data(self):
        """Method to get flat manager line"""

    @abstractmethod
    def get_previous_month_headcount_data(self):
        """Method to get previous month's EC headcount data"""



