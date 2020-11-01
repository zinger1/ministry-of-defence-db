from SRC import db_api
from dataclasses import dataclass
from dataclasses_json import dataclass_json
import shelve
import os
from typing import Any, Type

import operator

operators = {
    '<': operator.lt,
    '<=': operator.le,
    '=': operator.eq,
    '==': operator.eq,
    '!=': operator.ne,
    '>=': operator.ge,
    '>': operator.gt
}


@dataclass_json
@dataclass
class DBField(db_api.DBField):
    name: str
    type: Type


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any


# DB_ROOT = Path('db_files')


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    def __init__(self, name, fields, key_field_name):
        path_file = os.path.join('db_files', name)
        os.makedirs(path_file)
        table_file = shelve.open(os.path.join(f'{path_file}\\{name}.db'))
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
        if len(values) != len(self.fields):
            raise ValueError
        list_keys = values.keys()
        for field in self.fields:
            if field.name not in list_keys:
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
        try:
            for key in table_file.keys():
                flag = 1
                for criterion in criteria:
                    str_operator = operators.get(criterion.operator)
                    if criterion.field_name == self.key_field_name and not str_operator(key, str(criterion.value)):
                        flag = 0
                    elif criterion.field_name in self.fields and not str_operator(table_file[key][criterion.field_name],
                                                                                  str(criterion.value)):
                        flag = 0
                if flag:
                    table_file.pop(key)
                else:
                    flag = 1
        finally:
            table_file.close()

    def get_record(self, key):
        table_file = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)

        try:
            if not table_file.get(str(key)):
                table_file.close()
                raise ValueError
            result = {self.key_field_name: key}
            result.update(table_file[str(key)])
            return result
        finally:
            table_file.close()

    def update_record(self, key, values):
        table_file = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            if not table_file.get(str(key)):
                table_file.close()
                raise ValueError
            for key_ in values.keys():
                if not key_ in [field.name for field in self.fields]:
                    table_file.close()
                    raise ValueError
            for key_ in values.keys():
                table_file[str(key)][str(key_)] = values[str(key_)]
        finally:
            table_file.close()

    def query_table(self, criteria):
        table_file = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        list_records = list()
        try:
            for key in table_file.keys():
                flag = 1
                for criterion in criteria:
                    str_operator = operators.get(criterion.operator)
                    if criterion.field_name == self.key_field_name and not str_operator(key, str(criterion.value)):
                        flag = 0
                    elif not str_operator(table_file[key][criterion.field_name], str(criterion.value)):
                        flag = 0
                if flag:
                    list_records.append(self.get_record(key))
                else:
                    flag = 1
        finally:
            table_file.close()
            return list_records


# def create_index(self, field_to_index: str) -> None:
#     raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    __DICT_TABLES__ = {}

    def __init__(self):
        with shelve.open('DB', writeback=True) as db:
            for key in db:
                DataBase.__DICT_TABLES__[key] = DBTable(key, db[key][0], db[key][1])

    def create_table(self, table_name, fields, key_field_name):
        if table_name in DataBase.__DICT_TABLES__.keys():
            raise ValueError
        if key_field_name not in [field.name for field in fields]:
            raise ValueError
        with shelve.open('DB', writeback=True) as db:
            db[table_name] = [fields, key_field_name]
        db_table = DBTable(table_name, fields, key_field_name)
        DataBase.__DICT_TABLES__[table_name] = db_table
        return DataBase.__DICT_TABLES__[table_name]

    def num_tables(self) -> int:
        return len(DataBase.__DICT_TABLES__)

    def get_table(self, table_name: str) -> DBTable:
        if DataBase.__DICT_TABLES__.get(table_name):
            return DataBase.__DICT_TABLES__[table_name]
        raise ValueError

    def delete_table(self, table_name: str) -> None:
        if DataBase.__DICT_TABLES__.get(table_name):
            os.remove(os.path.join('db_files', table_name + '.db.bak'))
            os.remove(os.path.join('db_files', table_name + '.db.dat'))
            os.remove(os.path.join('db_files', table_name + '.db.dir'))
            DataBase.__DICT_TABLES__.pop(table_name)
        else:
            raise ValueError

    def get_tables_names(self):
        if DataBase.__DICT_TABLES__.keys():
            return list(DataBase.__DICT_TABLES__.keys())
        return []

# def query_multiple_tables(
#         self,
#         tables: List[str],
#         fields_and_values_list: List[List[SelectionCriteria]],
#         fields_to_join_by: List[str]
# ) -> List[Dict[str, Any]]:
#     raise NotImplementedError
