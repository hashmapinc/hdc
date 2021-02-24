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
from unittest import TestCase

import yaml


class TestOracleDAO(TestCase):
    __standard_profile_yml_path = Path.home() / ".hdc" / "profile.yml"
    __local_profile_path = "./profile.yml"

    def setUp(self) -> None:
        profile_dict = {
            'oracle_localhost_profile': {'host': 'localhost',
                                         'port': 1521,
                                         'database': 'XEPDB1',
                                         'user': 'hdc_user',
                                         'password': 'hdc12345',
                                         'driver': {
                                             'name': 'oracle.jdbc.OracleDriver',
                                             'path': '/Users/chinmayeelakkad/Library/DBeaverData/drivers'
                                                     '/maven/maven-central/com.oracle.database.jdbc'
                                                     '/ojdbc8-12.2.0.1.jar '
                                         }
                                         }
        }

        with open(TestOracleDAO.__local_profile_path, 'w') as fout:
            fout.write(yaml.dump(profile_dict))

    def tearDown(self) -> None:
        if Path.exists(Path(TestOracleDAO.__local_profile_path)):
            os.remove(TestOracleDAO.__local_profile_path)

    # def test_connection_established(self):
    #     dao = Oracle(connection='oracle_localhost_profile')
    #     a_connection = dao.get_connection()
    #     self.assertTrue(dao._test_connection(a_connection),
    #                     'Failed to establish a connection with configured Oracle DB')
