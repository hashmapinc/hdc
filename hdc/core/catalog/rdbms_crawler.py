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
"""
#TODO: Module description
"""

from hdc.core.catalog.crawler import Crawler


class RdbmsCrawler(Crawler):

    def run(self) -> tuple:
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    @classmethod
    def _get_database_names(cls, conn, query_string) -> list:
        databases = []
        cursor = conn.cursor()
        cursor.execute(query_string)
        result = cursor.fetchall()

        for row in result:
            databases.append(row[0])

        return databases

    @classmethod
    def _get_schema_names_by_db(cls, conn, query_string) -> list:
        schemas = []
        cursor = conn.cursor()
        cursor.execute(query_string)
        result = cursor.fetchall()

        for row in result:
            schemas.append(row[0])

        return schemas


    @classmethod
    def _get_tables_by_db(cls, conn, query_string) -> dict:
        tables = {}

        cursor = conn.cursor()
        cursor.execute(query_string)
        result = cursor.fetchall()

        for row in result:
            table_name = f"{row[0]}.{row[1]}.{row[2]}"  # ignoring name collisions across multiple db's for now
            if table_name not in tables:
                tables[table_name] = []

            column = {
                'database': row[0],
                'schema': row[1],
                'name': row[2],
                'columnName': row[3],
                'columnType': row[4],
                'columnSize': row[5],
                'notNull': row[6],
                'default': row[7]
            }
            tables[table_name].append(column)
        return tables

