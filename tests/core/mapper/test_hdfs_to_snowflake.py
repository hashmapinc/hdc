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

from unittest import TestCase

from pandas import DataFrame
from providah.factories.package_factory import PackageFactory as providah_pkg_factory

from hdc.core.map.hdfs_to_snowflake import HdfsToSnowflake
from hdc.core.map.mapper import Mapper


class TestHdfsToSnowflake(TestCase):
    print_sample = False

    def setUp(self) -> None:
        self._app_config = {
            "mappers": {
                "hdfs":
                    {"snowflake": {
                        "type": "HdfsToSnowflake",
                        "conf": {
                            "schema": {
                                "department": {"type": "record", "name": "department", "fields": [
                                    {"name": "column1", "type": "string"}]},
                                "resources": {"type": "record", "name": "resources", "fields": [
                                    {"name": "column1", "type": "string"}]}
                            }
                        }
                    }
                    }
            }
        }

        self._mapper: Mapper = providah_pkg_factory.create(key=self._app_config['mappers']['hdfs']['snowflake']['type'],
                                                           configuration={'conf': (self._app_config['mappers']['hdfs']
                                                           ['snowflake']).get('conf', None)
                                                                          }
                                                           )

    def test_mapper_instantiation(self):
        self.assertIsNotNone(self._mapper)
        self.assertIsInstance(self._mapper, HdfsToSnowflake)

    def test_map_assets(self):
        # Execute the method to test
        data_dict = [
            {"PARENT": "/home/dummy/resources", "FILE ASSET": "file1.csv"},
            {"PARENT": "/home/dummy/resources", "FILE ASSET": "file2.csv"},
            {"PARENT": "/home/dummy/resources/department", "FILE ASSET": "department.csv"}
        ]

        resources_table_schema = self._mapper._conf['schema']['resources']
        dept_table_schema = self._mapper._conf['schema']['department']
        catalog_dataframe = DataFrame(data_dict, columns=['PARENT', 'FILE ASSET'])
        sql_ddl_list = self._mapper.map_assets(catalog_dataframe)

        # Make assertions
        self.assertIsNotNone(sql_ddl_list)
        self.assertListEqual(sql_ddl_list, ['CREATE DATABASE IF NOT EXISTS "HDFS"',
                                            'CREATE SCHEMA IF NOT EXISTS "HDFS"."DEFAULT"',
                                            f"CREATE OR REPLACE TABLE HDFS.DEFAULT.RESOURCES ({', '.join([' '.join([field['name'], field['type']]) for field in resources_table_schema['fields']])})",
                                            f"CREATE OR REPLACE TABLE HDFS.DEFAULT.DEPARTMENT ({', '.join([' '.join([field['name'], field['type']]) for field in dept_table_schema['fields']])})"])

        if TestHdfsToSnowflake.print_sample:
            from pprint import pprint
            pprint(sql_ddl_list)
