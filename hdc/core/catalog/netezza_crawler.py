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

from hdc.core.catalog.rdbms_crawler import RdbmsCrawler


class NetezzaCrawler(RdbmsCrawler):

    def __init__(self, **kwargs):
        self.__logger = self.__get_logger()
        self.__dao_conf = kwargs.get('dao_conf')

    @classmethod
    def __get_logger(cls):
        return logging.getLogger(cls.__name__)

    def run(self) -> tuple:

        try:
            connector = providah_pkg_factory.create(key=self.__dao_conf['class_type'],
                                                    configuration={'connection': self.__dao_conf['conn_profile_name']})
            with connector.connection as conn:
                databases = self._get_database_names(conn)
                schemas = []
                tables = {}
                for db in databases:
                    schemas.extend(self._get_schema_names_by_db(db, conn))
                    tables.update(self._get_tables_by_db(db, conn))
                return databases, schemas, tables
        except:
            raise ValueError(
                "Unable to connect to Netezza. Please check if source is up. Check connection profile configuration: %s" % self.__dao_conf['conn_profile_name'])

    @classmethod
    def _get_database_names(cls, conn) -> list:
        query_string = "SELECT DATABASE FROM _V_DATABASE WHERE DATABASE <> 'SYSTEM'"
        return RdbmsCrawler._get_database_names(conn, query_string)

    @classmethod
    def _get_schema_names_by_db(cls, database, conn) -> list:
        query_string = f"SELECT DISTINCT SCHEMA FROM {database}.._V_SCHEMA"  # WHERE OBJTYPE = 'TABLE'"
        return RdbmsCrawler._get_schema_names_by_db(conn, query_string)

    @classmethod
    def _get_tables_by_db(cls, database, conn) -> dict:
        query_string = f"SELECT DATABASE, SCHEMA, NAME, ATTNAME, FORMAT_TYPE, ATTLEN, ATTNOTNULL, COLDEFAULT " \
                       f"FROM {database}.._V_RELATION_COLUMN " \
                       f"WHERE DATABASE <> 'SYSTEM' AND TYPE = 'TABLE' ORDER BY SCHEMA, NAME, ATTNUM ASC"
        return RdbmsCrawler._get_tables_by_db(conn, query_string)
