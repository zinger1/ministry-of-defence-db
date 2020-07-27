import db_api
from dataclasses import dataclass
from dataclasses_json import dataclass_json
import shelve
import os


# @dataclass_json
# @dataclass
# class DBField(db_api.DBField):
#     name: str
#     type: Type


# @dataclass_json
# @dataclass
# class SelectionCriteria(db_api.SelectionCriteria):
#     field_name: str
#     operator: str
#     value: Any

# from dataclasses import dataclass
# from pathlib import Path
# from typing import Any, Dict, List, Type

# from dataclasses_json import dataclass_json

# DB_ROOT = Path('db_files')

@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    def __init__(self, name, fields, key_field_name):
        path_file = os.path.join(db_api.DB_ROOT, name + '.db')
        table_file = shelve.open(path_file)
        table_file.close()
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

    def count(self) -> int:
        table_file = shelve.open(os.path.join('db_files', self.name + '.db'))
        try:
            count_table = len(table_file)
        finally:
            table_file.close()
        return count_table

    def insert_record(self, values):
        if not len(values) == len(self.fields):
            raise ValueError
        for field in self.fields:
            if field.name not in values.keys():
                raise ValueError
        table_file = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            if table_file.get(str(values[self.key_field_name])):
                table_file.close()
                raise ValueError          
            table_file[str(values.pop(self.key_field_name))] = values          
        finally:
            table_file.close()
        
    def delete_record(self, key):
        shelve_file = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            if not shelve_file.get(str(key)):
                shelve_file.close()
                raise ValueError
            shelve_file.pop(str(key))
        finally:
            shelve_file.close()

    def delete_records(self, criteria):
        table_file = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        # for criterion in criteria:
        #     if criterion.field_name == self.key_field_name and :
        #         table_file.pop(criterion.field_name)

        for key in table_file.keys():
            for criterion in criteria:
                # search_key = None
                if criterion.field_name == self.key_field_name and eval(f'{key} {criterion.operator} {criterion.value}'):
                    table_file.pop(key)
                # if criterion.field_name == self.key_field_name  or criterion.field._name in key.get(self.key_field_name).keys():

    def get_record(self, key):
        raise NotImplementedError

    # def update_record(self, key: Any, values: Dict[str, Any]) -> None:
    #     raise NotImplementedError

    # def query_table(self, criteria: List[SelectionCriteria]) \
    #         -> List[Dict[str, Any]]:
    #     raise NotImplementedError

    # def create_index(self, field_to_index: str) -> None:
    #     raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    def __init__(self):
        self.dict_tables = {}

    def create_table(self, table_name, fields, key_field_name):
        if self.dict_tables.get(table_name):
            raise ValueError
        db_table = DBTable(table_name, fields, key_field_name)
        if not self.dict_tables.get(table_name):
            raise ValueError
        self.dict_tables[table_name] = db_table
        return self.dict_tables[table_name]

    def num_tables(self) -> int:
        return len(self.dict_tables)

    def get_table(self, table_name: str) -> DBTable:
        if self.dict_tables.get(table_name):
            return self.dict_tables[table_name]
        raise ValueError

    def delete_table(self, table_name: str) -> None:
        if self.dict_tables[table_name]:
            self.dict_tables.pop(table_name)
            os.remove(os.path.join('db_files', table_name + '.db'))
        else:
            raise ValueError

    def get_tables_names(self):
        if(self.dict_tables.keys()):
            return list(self.dict_tables.keys())
        return []


    # def query_multiple_tables(
    #         self,
    #         tables: List[str],
    #         fields_and_values_list: List[List[SelectionCriteria]],
    #         fields_to_join_by: List[str]
    # ) -> List[Dict[str, Any]]:
    #     raise NotImplementedError


