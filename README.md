# Reporting Tool for Recurring HR Reports

This is a reporting codebase created to simplify the recurring report creation in one of my previous projects.

---
  This tool was created in 2021 as the second version of the code used in the reporting project I was working in.
The main goal was to create a substitute of the existing recurring report generation. The existing solution used when started working on this task was using SAP Business Warehouse Excel add-in in separate templates and SuccessFactors report creator to generate the recurring HR reports (these were mostly daily, weekly, bi-weekly, monthly, quarterly and yearly reports).

  The first version of the tool was following more of a functional structure. This version have been built with with an object oriented structure, tryin go apply the clean code principles I'd learned not long before starting the refactoring of the first version.

---
The tool is written in Python, heavily utilizing the Pandas library for the data manipulation and the input-output processes.
The main components are:
- DataLoader class,
- DataProvider class,
- ReportDefinitions module (with report definitions implementing the IReportDefinition metaclass),
- DataTransformation module,
- SupportFunctions module,
- Tests

---
#### Copyrights
This tool was created during corporate employment. When I left the company an agreement was made to let the project continue to use the codebase, however this agreement did not include the transfer of ownership. All the input and template files containing sensitive information and company property have been removed.

---
## Component descriptions
### DataLoader
The DataLoader class has the responsibility to read the HR master data from a static Excel file and do the necessary cleaning and transformation procedures to prepare the different datasets (e.g. headcount, turnover, re-entry data, ect.) needed for the reports.
As direct data connection to EmployeeCentral backend or an existing SQL database with the data loaded was not permitted up until the point I left the company the class has to work with Excel input. Since it was a long term plan to have direct connction to the back end, the DataProvider class was implemented (next item on the descriptions) to actually feed the report generation with data, thus if a new DataLoader could have been made (loading data from the back end) the report definition classes would't needed to be refactored to accomodate the new data source.
### DataProvider class
The DataProvider class has the responsibility to provide the dataframes to the report definitions. The main idea behind it was to don't let the report creator methods have direct access to the DataLoader source data in order to avoid any change by mistake, thus jeopardize the subsequent outputs accuracy.
### ReportDefinitions module
These are the classes containing the details of the reports to be created. They implement the IReportDefiniton class which provides the functionality to:
- list the report generator methods(list_report_definitions),
- call all methods and generate all defined reports(generate_reports),
- based on the index provided by the list_report_definitions method call only specific report generating methods (generate_specific_reports)
### DataTransformation module
This module contains two classes designed to transform the base data into data objects needed for report creation, however too complicated to do it on report definition level:
- CreateManagerLineDataFrame class: responsible for creating the entire managerial line of each employee (manager, manager's manager, manager's manager's manager and so on),
- FlatFileGenerator: responsible for creating the flat organizational structure for each employee (the so called "flat file") that contains the entire organizational structure in one record per employee.
### SupportFunctions module
This is a collection of files and functions used in the report definitions not large/complex enough to have their own class in the DataTransformation module:
- decorators: currently containing one decorator function that outputs the time each report generation takes,
- export_dataframes_to_excel: contains the utility functions used to open, write and clean excel templates and save generated reports,
- extract_employee_data_changes: used to compare two DataFrames (from two different points of time) to see which employees had changes along the predefined dinemsions,
- filter_dataframes: functions to make filtering headcount or turnover data more readable,
- get_manager_data: responsible for joining a headcount DataFrame to itself to extract dimensions on managers from the headcount. It takes care of basic sense checking and formatting as well thus making the returned manager data easy to use once created,
- save_temporary_data: it was created for a specific report that needed headcount data from a certain workday, but was generated after that workday. As the past headcount data was constantly corrected, we needed a method that ran every day and saved a "frozen" dataset that could be used later. 
### Tests
The module contains unittests of the larger components of the tool and the report definition classes.

## Installation and usage
This tool does not require installation to use beside the installation of dependencies listed in requirements.txt. Two files contain the source data and template/output destination paths that need to be modified in order to use the project. These files are located in DataLoader\data_file_paths.py and ReportDefinitions\paths.py.


