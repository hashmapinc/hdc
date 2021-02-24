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
import logging
import time
import traceback
from contextlib import contextmanager
from functools import reduce
from pathlib import Path

from hdc.core.dao.dao import DAO
from hdc.utils.file_parsers import yaml_parser


class RdbmsDAO(DAO):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._max_attempts = 3
        self._timeout_seconds = 10
        self._connection = kwargs.get('connection')
        self._logger = self._get_logger()

    def get_conn_profile_key(self, keys, default=None):
        """
        Fetch values for keys (nested or not) from the connection profile except the password key.
        Nested keys should be '.' delimited.

        :param keys:
        :param default:
        :return:
        """
        connection_profile = self._read_connection_profile(self._connection)
        if 'password' not in keys.split('.'):
            return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."),
                          connection_profile)
        else:
            return default

    @contextmanager
    def get_connection(self):
        """
        Obtain a context managed snowflake connection

        Returns: Snowflake connection

        Raises:
            ConnectionError: Snowflake connection could not be established

        """
        connection_established = False
        connection_attempts = 0
        timeout = self._timeout_seconds
        connection = None

        # Attempt to connect
        while connection_attempts < self._max_attempts:
            try:
                connection_profile = self._read_connection_profile(self._connection)
                connection = self._attempt_to_connect(connection_profile)

                # Test if connection has been established and break
                if self._test_connection(connection):
                    # self._logger.info(f"Connection successfully established")
                    connection_established = True
                    break

                # If not, re-attempt to connect after a brief sleep
                timeout, connection_attempts = self._sleep_and_increment_counter(timeout, connection_attempts)

            except Exception as e:
                raise e
                # connection_attempts, connection_established = self._manage_exception(timeout,
                #                                                                      connection_attempts,
                #                                                                      connection_established)

        if not connection_established:
            raise ConnectionError('Could not connect to the database; Exhausted connection attempts!')

        yield connection

        connection.close()

    @staticmethod
    def _get_profile_path():
        return Path.home() / '.hdc' / 'profile.yml'

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
            timeout *= self._timeout_seconds  # TODO: sleep timeout could be increased by the same factor
        return timeout, connection_attempt_count

    def _manage_exception(self, timeout, connection_attempt_count, connection_established):
        if connection_attempt_count < self._max_attempts:
            error_message = f'Failed to connect to database; Caught exception: ' \
                            f'{traceback.format_exc()}' \
                            f'Re-attempting to connect ...'
            self._logger.error(error_message)
            _, connection_attempt_count = self._sleep_and_increment_counter(timeout, connection_attempt_count)
        else:
            connection_established = True
        return connection_attempt_count, connection_established

    def _attempt_to_connect(self, connection_profile):
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')

    def _read_connection_profile(self, connection_profile_name) -> dict:
        if Path.exists(self._get_profile_path()):
            profile_yaml = yaml_parser(yaml_file_path=self._get_profile_path())

            if connection_profile_name not in profile_yaml:
                raise KeyError(f'{connection_profile_name} not configured in profile.yml')
            else:
                connection_profile = profile_yaml[connection_profile_name]  # Masking the outer local variable
        else:
            raise FileNotFoundError(
                f'Could not locate the profile.yml file. Please refer to the README for setup directions.')

        return connection_profile

    @staticmethod
    def _validate_connection_profile(profile, required_keys):
        return (all([key in profile.keys() for key in required_keys]),
                [key for key in required_keys if key not in profile.keys()])
