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
# TODO: Module description
"""

#import pyodbc

from hdc.core.dao.netezza import Netezza


class NetezzaODBC(Netezza):

    def __build_connection_string(self, config: dict) -> dict:
        return dict(driver=config['driver'],
                    connection_string=f"DRIVER={config['driver']};SERVER={config['host']};"
                                      f"PORT={config['port']};DATABASE={config['database']};"
                                      f"UID={config['user']};PWD={config['password']};")

    def __attempt_to_connect(self, conn_conf):
        # odbc_compliant_config = self.__build_connection_string(conn_conf)
        # return pyodbc.connect(odbc_compliant_config['connection_string'])
        pass