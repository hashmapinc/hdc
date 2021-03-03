#  Copyright © 2020 Hashmap, Inc
#  #
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  #
#      http://www.apache.org/licenses/LICENSE-2.0
#  #
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import pandas as pd

from hdc.core.map.mapper import Mapper


class HiveToSnowflake(Mapper):
    # TODO: Add support for collection data types from Hive like: Array, Map, Struct
    data_type_map = {
        "VARCHAR": "VARCHAR",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT|TINYINT",
        "BINARY": "BINARY",
        "BOOLEAN": "BOOLEAN",
        "CHAR": "CHAR",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "DECIMAL": "DECIMAL|NUMERIC",
        "DOUBLE": "(DOUBLE)+(PRECISION)?",
        "FLOAT": "FLOAT",
        "INT": "((INT)EGER)"
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._logger = self._get_logger()

    def map_assets(self, df_catalog: pd.DataFrame) -> list:
        unique_databases = df_catalog['DATABASE_NAME'].unique()

        # DDL for all unique databases
        sql_ddl_list = self.__map_databases(unique_databases)

        # DDL for all schemas under each unique database
        for db in unique_databases:
            db_filter = df_catalog['DATABASE_NAME'] == db
            sql_ddl_list.extend(self.__map_schemas(db, df_catalog[db_filter]['SCHEMA_NAME'].unique()))

        # DDL for all tables under each unique database and schema
        sql_ddl_list.extend(self.__map_tables(df_catalog))

        return sql_ddl_list

    @staticmethod
    def __map_databases(databases) -> list:
        return [f'CREATE DATABASE IF NOT EXISTS "{db.upper()}"' for db in databases]

    @staticmethod
    def __map_schemas(database, schemas) -> list:
        return [f'CREATE SCHEMA IF NOT EXISTS "{database.upper()}"."{schema.upper()}"' for schema in schemas]

    @staticmethod
    def __map_data_types(src_data_type: pd.Series) -> pd.Series:
        target_data_type = src_data_type
        for target_type, source_type in HiveToSnowflake.data_type_map.items():
            target_data_type = target_data_type.str.replace(source_type, target_type, case=False)
        return target_data_type

    def __map_tables(self, df_catalog: pd.DataFrame) -> list:
        sql_ddl = []

        # TODO: Filter out rows where column_type is un-supported and log

        df_catalog['TARGET_COLUMN_TYPE'] = self.__map_data_types(df_catalog['COLUMN_TYPE'])
        df_catalog['COLUMN_DESC'] = df_catalog['COLUMN_NAME'] + ' ' + df_catalog['TARGET_COLUMN_TYPE']

        df_table_group = df_catalog[['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_DESC']].groupby(
            ['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME'])

        for name, group in df_table_group:
            sql_ddl.append(f"CREATE OR REPLACE TABLE {'.'.join(name).upper()} "
                           f"({','.join(list(group['COLUMN_DESC'])).upper()})")

        return sql_ddl
