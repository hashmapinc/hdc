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

from hdc.core.create.rdbms_creator import RdbmsCreator
from hdc.core.dao.rdbms_dao import RdbmsDAO


class SnowflakeCreator(RdbmsCreator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__logger = self._get_logger()
        self.__dao_conf = kwargs.get('dao_conf')

    def replicate_structures(self, sql_list):
        dao: RdbmsDAO = providah_pkg_factory.create(key=self.__dao_conf['class_name'],
                                                    configuration={'connection': self.__dao_conf['conn_profile_name']})
        self._execute_update(dao, sql_list)
