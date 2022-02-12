from ReportingSourceCode.DataLoader.DataLoader import DataLoader
from ReportingSourceCode.DataProvider.DataProvider import DataProvider
from ReportingSourceCode.ReportDefinitions.RD2ndDayMonthly import RD2ndDayMonthly
from ReportingSourceCode.ReportDefinitions.RD3rdDayMonthly import RD3rdDayMonthly
from ReportDefinitions.paths import output_path
import unittest
import pandas as pd
import os


class TestRD1stDayMonthly(unittest.TestCase):
    rds_to_test = [RD2ndDayMonthly, RD3rdDayMonthly]
    for report_def in rds_to_test:
        @classmethod
        def setUpClass(cls) -> None:
            cls.DataLoader = DataLoader()
            cls.DataProvider = DataProvider(cls.DataLoader)
            cls.Reports = report_def(cls.DataProvider)
            cls.report_list = cls.Reports.list_report_definitions()
            cls.generated_reports = cls.Reports.generate_reports()

        def test_list_report_definitions_returns_list(self):
            self.assertIsInstance(self.Reports.list_report_definitions(), list)

        def test_list_report_definitions_returns_correct_list_of_methods(self):
            test_report_list = [func for func in dir(report_def) if
                                callable(getattr(report_def, func)) & func.startswith("rec")]
            self.assertEqual(self.Reports.list_report_definitions(), test_report_list)

        def test_generate_reports_returns_list(self):
            self.assertIsInstance(self.generated_reports, list)

        def test_generate_reports_returns_list_with_len_equal_to_list_report(self):
            self.assertEqual(len(self.generated_reports), len(self.report_list))

        def test_generate_reports_outputs_all_excel_files_from_definitions(self):
            generated_reports_set = set(self.generated_reports)
            files_in_output_folder_set = set([file for file in os.listdir(output_path) if file in self.generated_reports])

            self.assertEqual(files_in_output_folder_set, generated_reports_set)
