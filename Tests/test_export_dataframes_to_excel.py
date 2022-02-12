import unittest
import pandas as pd
import numpy as np

from SupportFunctions.export_dataframes_to_excel import export_df
from ReportingSourceCode.ReportDefinitions.paths import template_path


class TestExportDataframesToExcel(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.output_df = pd.DataFrame(np.random.randn(10, 4), columns=list('ABCD'))
        cls.df_dict = {'Report': cls.output_df}
        cls.path = template_path + '03_TEMPLATE TestExportDataframesToExcel.xlsx'
        cls.o_name = 'unittest output - TestExportDataframesToExcel'

    def test_export_df_returns_string(self):
        output_string = export_df(self.output_df, self.path, self.o_name)
        self.assertIsInstance(output_string, str)



