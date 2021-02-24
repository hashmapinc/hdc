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
from hdc.utils.misc import get_app_config


class AssetMapper:

    def __init__(self, **kwargs):
        source = kwargs.get('source')
        destination = kwargs.get('destination')
        app_config = get_app_config(kwargs.get('app_config', None))

        self._crawler: Crawler = providah_pkg_factory.create(key=app_config['sources'][source]['class_name'],
                                                             configuration={'dao_conf': {
                                                                 'class_name': app_config['sources'][source]['dao'],
                                                                 'conn_profile_name': app_config['connection_profiles'][
                                                                     source]
                                                             }})

        self._mapper: Mapper = providah_pkg_factory.create(key=app_config['mappers'][source][destination])

        self._creator: Creator = providah_pkg_factory.create(key=app_config['destinations'][destination]['class_name'],
                                                             configuration={'dao_conf':
                                                                 {
                                                                     'class_name':
                                                                         app_config['destinations'][destination][
                                                                             'dao'],
                                                                     'conn_profile_name':
                                                                         app_config['connection_profiles'][
                                                                             destination]
                                                                 }})

    def map_assets(self) -> bool:
        success = False
        try:
            catalog_dataframe = self._crawler.obtain_catalog()
            if catalog_dataframe is not None:
                sql_ddl_list = self._mapper.map_assets(catalog_dataframe)
                self._creator.replicate_structures(sql_ddl_list)
                success = True
        except Exception as e:
            raise e
            success = False
        return success
