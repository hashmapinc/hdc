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
# TODO : Module description
"""
import jaydebeapi

from hdc.core.dao.netezza import Netezza
from hdc.utils.file_parsers import yaml_parser


class NetezzaJDBC(Netezza):

    def _validate_configuration(self, required_keys) -> bool:
        is_valid = super().__validate_configuration(required_keys)

        profile_yaml = yaml_parser(yaml_file_path=self._get_profile_path())
        conn_conf = profile_yaml[self._connection_name]

        if is_valid:
            required_keys = ['name', 'path']
            return all([key in conn_conf['driver'].keys() for key in required_keys])

        return is_valid

    def __build_connection_string(self, config: dict):
        return dict(driver_name=config['driver']['name'],
                    driver_location=config['driver']['path'],
                    connection_string=f"jdbc:netezza://{config['host']}:{config['port']}/{config['database']}",
                    user=config['user'],
                    password=config['password'])

    def __attempt_to_connect(self, conn_conf):
        jdbc_compliant_config = self.__build_connection_string(conn_conf)
        return jaydebeapi.connect(jdbc_compliant_config['driver_name'],
                                 jdbc_compliant_config['connection_string'],
                                 {
                                     'user': jdbc_compliant_config['user'],
                                     'password': jdbc_compliant_config['password']
                                 },
                                 jars=jdbc_compliant_config['driver_location'])
