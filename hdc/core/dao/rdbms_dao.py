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
import time
import traceback
from contextlib import contextmanager
from pathlib import Path

from hdc.core.dao.dao import DAO
from hdc.utils.file_parsers import yaml_parser


class RdbmsDAO(DAO):

    def __init__(self, **kwargs):
        self._max_attempts = 3
        self._timeout_seconds = 10
        self._connection_name = kwargs.get('connection')
        if not self._validate_configuration(self._connection_name, required_keys=['user', 'password', 'host', 'port', 'database', 'driver']):
            raise ConnectionError(f'Missing Configuration key for {type(self).__name__}. Please check profile.yml file. '
                                  f'The list of required keys can be found in dao/{type(self).__name__}.py in _validate_configuration method.')


    @property
    def connection(self):
        return self.get_connection()

    @contextmanager
    def get_connection(self, database_connector):
        """
        Obtain a context managed snowflake connection

        Returns: Snowflake connection

        Raises:
            ConnectionError: Snowflake connection could not be established

        """
        # Check if connection is valid, and if it isn't, then attempt create it with exponential fall of up to 3 times.
        connection_established = False
        connection_attempts = 0
        timeout = self._timeout_seconds
        connection = None

        profile_yaml = yaml_parser(yaml_file_path=self._get_profile_path())
        conn_conf = profile_yaml[self._connection_name]

        while connection_attempts < self._max_attempts:
            try:
                connection = self.__attempt_to_connect(conn_conf)

                # Test if connection has been established and break
                if self._test_connection(connection):
                    connection_established = True
                    break

                # If not, re-attempt to connect after a brief sleep
                timeout, connection_attempts = self._sleep_and_increment_counter(timeout, connection_attempts)

            except Exception:
                connection_attempts, connection_established = self._manage_exception(timeout,
                                                                                     connection_attempts,
                                                                                     connection_established)

        if not connection_established:
            raise ConnectionError('Could not connect to the database; Exhausted connection attempts!')

        yield connection

        connection.close()

    def _test_connection(self, connection) -> bool:
        """
        Validate that the connection is valid to Snowflake instance

        Returns: True if connection is valid, False otherwise

        """
        if not connection:
            return False

        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if not len(cursor.fetchone()) > 0:
                return False

            return True

    def _sleep_and_increment_counter(self, timeout, connection_attempt_count):
        connection_attempt_count += 1
        if connection_attempt_count < self._max_attempts:
            time.sleep(self._timeout_seconds)
            timeout *= self._timeout_seconds
        return timeout, connection_attempt_count

    def _manage_exception(self, timeout, connection_attempt_count, connection_established):
        if connection_attempt_count < self._max_attempts:
            error_message = f'Failed to connect to database; Caught exception: ' \
                            f'{traceback.format_exc()}' \
                            f'Re-attempting to connect ...'
            self._logger.error(error_message)
            connection_attempt_count = self._sleep_and_increment_counter(timeout, connection_attempt_count)
        else:
            connection_established = True
        return connection_attempt_count, connection_established

    def _validate_configuration(self, required_keys) -> bool:
        profile_yaml = yaml_parser(yaml_file_path=self._get_profile_path())
        conn_conf = profile_yaml[self._connection_name]

        is_valid = all([key in conn_conf.keys() for key in required_keys])

        return is_valid

    def __attempt_to_connect(self, conn_conf):
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')


    def _get_profile_path(self):
        return Path.home() / '.hdc' / 'profile.yml'
