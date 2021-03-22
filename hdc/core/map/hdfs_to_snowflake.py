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


class HdfsToSnowflake(Mapper):
    avro_to_snowflake_data_type_map = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__logger = self._get_logger()

    def map_assets(self, df_catalog) -> list:
        # Default database for all HDFS data assets
        sql_ddl_list = self.__map_databases(["HDFS"])

        # Default schema for all HDFS data assets
        sql_ddl_list.extend(self.__map_schemas("HDFS", ["DEFAULT"]))

        # DDL for all tables under each unique database and schema
        sql_ddl_list.extend(self.__map_tables(df_catalog))

        return sql_ddl_list

    @staticmethod
    def __map_databases(databases) -> list:
        return [f'CREATE DATABASE IF NOT EXISTS "{db.upper()}"' for db in databases]

    @staticmethod
    def __map_schemas(database, schemas) -> list:
        return [f'CREATE SCHEMA IF NOT EXISTS "{database.upper()}"."{schema.upper()}"' for schema in schemas]

    def __map_tables(self, df_catalog: pd.DataFrame) -> list:
        sql_ddl = []

        # Extract the first common ancestor (directory)
        df_catalog['TABLE_NAME'] = df_catalog['PARENT'].map(self.__get_path_name)

        # Map the first common ancestor as a table and look-up its schema from the mapper conf
        for table in df_catalog['TABLE_NAME'].unique():
            try:
                table_schema = self._conf['schema'][table]
                sql_ddl.append(f"CREATE OR REPLACE TABLE {'.'.join(['HDFS', 'DEFAULT', table]).upper()} "
                               f"("
                               f"{', '.join([' '.join([field['name'], field['type']]) for field in table_schema['fields']])}"
                               f", CK_SUM VARCHAR"
                               f")")
            except KeyError:
                self.__logger.error(f"Either no schema configured for table '{table}' or configuration is incorrect")

        return sql_ddl

    @staticmethod
    def __get_path_name(file_path):
        from pathlib import Path
        return Path(file_path).name
