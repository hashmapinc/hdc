# Copyright © 2020 Hashmap, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
#TODO: Module description
"""

import logging

from providah.factories.package_factory import PackageFactory as providah_pkg_factory

from hdc.core.create.rdbc_creator import RdbmsCreator


class SnowflakeCreator(RdbmsCreator):

    @classmethod
    def __get_logger(cls):
        return logging.getLogger(cls.__name__)

    def __init__(self, **kwargs):
        self.__logger = self.__get_logger()
        self.__dao_conf = kwargs['dao_conf']

    def run(self, sql_list, database_sql, schema_sql, table_sql):
        try:
            connector = providah_pkg_factory.create(key=self.__dao_conf['class_type'],
                                                    configuration={'connection': self.__dao_conf['conn_profile_name']})

            # Skip creating databases for now

            # Create the Schema(s) in the target system
            self.__execute_sql(connector, schema_sql)

            # Create the tables in the target system
            self.__execute_sql(connector, table_sql)

        except:
            raise ValueError(
                "Unable to connect to Snowflake. Please check if source is up. Check the configuration: %s" % self._connection_name)


    def __execute_sql(self, connector, sql_list):
        with connector.connection as conn:
            cursor = conn.cursor()
            for sql in sql_list:
                self.__logger.info("executing SQL: %s", sql)
                cursor.execute(sql)

