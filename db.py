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

    # def count(self) -> int:
    #     return ra NotImplementedError

    # def insert_record(self, values: Dict[str, Any]) -> None:
    #     raise NotImplementedError

    # def delete_record(self, key: Any) -> None:
    #     raise NotImplementedError

    # def delete_records(self, criteria: List[SelectionCriteria]) -> None:
    #     raise NotImplementedError

    # def get_record(self, key: Any) -> Dict[str, Any]:
    #     raise NotImplementedError

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
        return self.dict_tables.keys()


    # def query_multiple_tables(
    #         self,
    #         tables: List[str],
    #         fields_and_values_list: List[List[SelectionCriteria]],
    #         fields_to_join_by: List[str]
    # ) -> List[Dict[str, Any]]:
    #     raise NotImplementedError


