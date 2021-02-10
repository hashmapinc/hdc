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
"""
#TODO: Module description
"""
from providah.factories.package_factory import PackageFactory as providah_pkg_factory

from hdc.core.catalog.crawler import Crawler
from hdc.core.create.creator import Creator
from hdc.core.map.mapper import Mapper


class AssetMapper:

    def __init__(self, **kwargs):
        source = kwargs.get('source')
        destination = kwargs.get('destination')
        mapper = kwargs.get('mapper')

        self._crawler: Crawler = providah_pkg_factory.create(key=source['class_type'],
                                                             configuration={'dao_conf': source['dao_conf']})

        self._mapper: Mapper = providah_pkg_factory.create(key=mapper['class_type'])

        self._creator: Creator = providah_pkg_factory.create(key=destination['class_type'],
                                                             configuration={'dao_conf': destination['dao_conf']})

    def map_assets(self) -> bool:
        """

        :return:
        """
        success = True
        try:
            data_tuple = self._crawler.run()
            if data_tuple:
                database_sql, schema_sql, table_sql =  self._mapper.run(databases=data_tuple[0], schemas=data_tuple[1], tables=data_tuple[2])
                self._creator.run(database_sql, schema_sql, table_sql)
        except Exception:
            success = False

        return success



