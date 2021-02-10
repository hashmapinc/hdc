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

import snowflake.connector

from hdc.core.dao.rdbms_dao import RdbmsDAO


class Snowflake(RdbmsDAO):

    def _validate_configuration(self) -> bool:
        # TODO
        return True

    def __attempt_to_connect(self, conn_conf):
        return snowflake.connector.connect(
                    user=conn_conf['user'],
                    password=conn_conf['password'],
                    account=conn_conf['account'],
                    authenticator=conn_conf['authenticator'],
                    warehouse=conn_conf['warehouse'],
                    database=conn_conf['database'],
                    schema=conn_conf['schema'],
                    role=conn_conf['role'],
                    login_timeout=1
                )
