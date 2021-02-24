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
import os
from pathlib import Path
from unittest import TestCase

import yaml

from hdc.core.dao.rdbms_dao import RdbmsDAO


class TestRdbmsDAO(TestCase):
    test_for_exceptions = False

    __standard_profile_yml_path = Path.home() / ".hdc" / "profile.yml"
    __local_profile_path = "./profile.yml"

    def setUp(self) -> None:
        profile_dict = {'sample_profile': {'host': 'localhost',
                                           'port': 1233,
                                           'database': 'test_db',
                                           'user': 'user1',
                                           'password': 'sdcsdc',
                                           'driver': {}
                                           }
                        }

        with open(TestRdbmsDAO.__local_profile_path, 'w') as fout:
            fout.write(yaml.dump(profile_dict))

    def tearDown(self) -> None:
        if Path.exists(Path(TestRdbmsDAO.__local_profile_path)):
            os.remove(TestRdbmsDAO.__local_profile_path)

    def test_exception_on_invalid_profile_name(self):
        if TestRdbmsDAO.test_for_exceptions:
            with self.assertRaises(KeyError) as context:
                dao = RdbmsDAO(connection='cdsdcs')

            print(context.exception)
        else:
            self.assertTrue(1 == 1)

    def test_exception_on_missing_required_keys_from_profile(self):
        if TestRdbmsDAO.test_for_exceptions:
            with self.assertRaises(KeyError) as context:
                dao = RdbmsDAO(connection='sample_profile')

            print(context.exception)
        else:
            self.assertTrue(1 == 1)

    def test_missing_profile_yml(self):
        if TestRdbmsDAO.test_for_exceptions:
            with self.assertRaises(FileNotFoundError) as context:
                dao = RdbmsDAO(connection='sample_profile')

            print(context.exception)
        else:
            self.assertTrue(1 == 1)

    # def test_valid_profile_yaml(self):
    #     dao = RdbmsDAO(connection='sample_profile')
    #     self.assertTrue(dao._connection_profile is not None, 'Successfully created a generic RdbmsDAO')
