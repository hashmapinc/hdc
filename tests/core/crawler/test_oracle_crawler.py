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


class TestOracleCrawler(TestCase):
    __standard_profile_yml_path = Path.home() / ".hdc" / "profile.yml"
    __local_profile_path = "./profile.yml"

    def setUp(self) -> None:
        profile_dict = {'oracle_localhost_profile': {'host': 'localhost',
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

        with open(TestOracleCrawler.__local_profile_path, 'w') as profile_yml_out:
            profile_yml_out.write(yaml.dump(profile_dict))

    def tearDown(self) -> None:
        if Path.exists(Path(TestOracleCrawler.__local_profile_path)):
            os.remove(TestOracleCrawler.__local_profile_path)

    # def test_oracle_cataloging(self):
    #     from providah.factories.package_factory import PackageFactory as providah_pkg_factory
    #
    #     app_config = {
    #         "oracle_crawler_test": {
    #             "class_type": "OracleCrawler",
    #             "dao_conf": {
    #                 "class_type": "Oracle",
    #                 "conn_profile_name": "oracle_localhost_profile",  # Must match a connection profile from profile.yml
    #                 "table_metadata_columns": {
    #                     "schema_name": "ALL_TABLES.OWNER",
    #                     "table_name": "ALL_TAB_COLUMNS.TABLE_NAME",
    #                     "column_name": "ALL_TAB_COLUMNS.COLUMN_NAME",
    #                     "column_type": "ALL_TAB_COLUMNS.DATA_TYPE",
    #                     "column_size": "CONCAT(CAST(ALL_TAB_COLUMNS.DATA_PRECISION AS CHAR), "
    #                                    "CAST(ALL_TAB_COLUMNS.DATA_SCALE AS CHAR))",
    #                     "not_null": "ALL_TAB_COLUMNS.NULLABLE"
    #                 }
    #             }
    #         }
    #     }
    #
    #     cataloger: Cataloger = providah_pkg_factory
    #
    #     df_catalog = cataloger.obtain_catalog()
    #
    #     self.assertIsNotNone(df_catalog)
    #
    #     cataloger.pretty_print(df_catalog)
