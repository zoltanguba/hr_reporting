from abc import ABCMeta, abstractmethod
from ReportingSourceCode.DataLoader.DataLoader import DataLoader
from ReportingSourceCode.DataProvider.IDataProvider import IDataProvider


class IReportDefinition(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, dataprovider: IDataProvider):
        self.dataprovider = dataprovider
        self.report_definition_dict = self.list_report_definitions()
        if not isinstance(self.dataprovider, IDataProvider):
            raise TypeError("Invalid DataProvider object provided: input must be of DataProvider class instance")

    def list_report_definitions(self) -> dict:
        """Looping through the report definitions and returning their name (?)"""
        report_list = [func for func in dir(self) if callable(getattr(self, func)) &
                       func.startswith("rec")]

        report_dict = {key: report_name for (key, report_name) in enumerate(report_list)}

        return report_dict

    def generate_reports(self) -> list:
        """Looping through and call the individual report definition methods"""
        generated_reports_list = []
        for report in self.report_definition_dict:
            callable_report = getattr(self, self.report_definition_dict[report])
            report_generated = callable_report()
            generated_reports_list.append(report_generated)

        return generated_reports_list

    def generate_specific_reports(self, report_number: list):
        """Generate specific reports out of the full list of definitions"""
        generated_reports_list = []
        for number in report_number:
            try:
                callable_report = getattr(self, self.report_definition_dict[number])
                report_generated = callable_report()
                generated_reports_list.append(report_generated)
            except KeyError:
                print(f"Key {number} does not exist in the Report Definition List")
                continue

        return generated_reports_list

