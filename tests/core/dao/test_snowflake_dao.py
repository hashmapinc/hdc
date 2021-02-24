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
import os
from pathlib import Path

import yaml

from tests.core.dao.test_rdbms_dao import TestRdbmsDAO


class TestSnowflakeDAO(TestRdbmsDAO):
    __local_profile_path = "./profile.yml"

    def setUp(self) -> None:
        profile_dict = {'snowflake_admin_schema': {
            'authenticator': 'externalbrowser',
            'account': '< account >',
            'role': '< role >',
            'warehouse': '< warehouse_name >',
            'database': '< database_name >',
            'schema': '< schema_name >'
        }
        }

        with open(TestSnowflakeDAO.__local_profile_path, 'w') as fout:
            fout.write(yaml.dump(profile_dict))

    def tearDown(self) -> None:
        if Path.exists(Path(TestSnowflakeDAO.__local_profile_path)):
            os.remove(TestSnowflakeDAO.__local_profile_path)

    # def test_valid_netezza_jdbc_profile(self):
    #     dao = Snowflake(connection='snowflake_dev')
    #     self.assertIsNotNone(dao)

    # def test_connection_established(self):
    #     dao = Snowflake(connection='snowflake_dev')
    #     a_connection = dao.get_connection()
    #     self.assertTrue(dao._test_connection(a_connection),
    #                     'Failed to establish a connection with configured Snowflake DB')
