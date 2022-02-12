import pandas as pd


class CreateManagerLineDataFrame:

    def __init__(self, base_dataframe):
        self.base_dataframe = base_dataframe
        self.identifier = 'Global ID'
        self.manager_map = self.create_employee_dict_with_manager_data()

    def create_employee_dict_with_manager_data(self):
        dataframe = self.base_dataframe[[self.identifier, 'Formal Name', 'Manager User Sys ID', 'Direct Manager']].copy()
        dataframe.drop_duplicates(inplace=True)
        manager_map = dict(
            zip(dataframe[self.identifier], [list(item) for item in zip(dataframe['Manager User Sys ID'],
                                                                    dataframe['Direct Manager'])]))
        return manager_map

    def get_manager_id_and_name_for_employee(self, employee_id, managers_list):
        try:
            id = int(self.manager_map[employee_id][0])
            name = self.manager_map[employee_id][1]
            manager_data = str(id) + " - " + name
        except Exception:
            return
        if id & id != employee_id:
            managers_list.append(manager_data)
            self.get_manager_id_and_name_for_employee(str(id), managers_list)

    def get_manager_line_for_employee(self, employee_id):
        managers_list = []
        self.get_manager_id_and_name_for_employee(employee_id, managers_list)
        managers_list.reverse()
        return managers_list

    def create_emoloyee_id_dataframe_for_manager_line(self):
        employee_list = list(self.base_dataframe[self.identifier])
        employee_dict = {self.identifier: employee_list}
        employee_df = pd.DataFrame(employee_dict)
        return employee_df

    def create_manager_line_dataframe(self, employee_list):
        managers_list = [self.get_manager_line_for_employee(x) for x in employee_list]
        manager_dict = {'Manager_line': managers_list}
        manager_line_df = pd.DataFrame(manager_dict)
        manager_line_df = manager_line_df['Manager_line'].apply(pd.Series)
        return manager_line_df

    def create_manager_line_to_employee_dataframe(self):
        employee_list = list(self.base_dataframe[self.identifier])
        employee_df = self.create_emoloyee_id_dataframe_for_manager_line()
        manager_line_df = self.create_manager_line_dataframe(employee_list)

        manager_df = pd.concat([employee_df, manager_line_df], axis='columns')
        return manager_df

