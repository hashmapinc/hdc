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

import logging

from hdc.core.catalog.rdbms_crawler import RdbmsCrawler


class OracleCrawler(RdbmsCrawler):

    def __init__(self, **kwargs):
        self._logger = self._get_logger()
        self.__connection_choice = kwargs.get('connection_name')

    @classmethod
    def _get_logger(cls):
        return logging.getLogger(cls.__name__)

    def run(self) -> tuple:
        # TODO: Add the implementation details for Oracle Crawler
        raise RuntimeError("This source is not yet supported")