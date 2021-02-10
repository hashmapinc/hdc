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
from argparse import ArgumentParser
from pathlib import Path
from pprint import pprint

from providah.factories.package_factory import PackageFactory as providah_pkg_factory

from hdc.core.asset_mapper import AssetMapper
from hdc.core.cataloger import Cataloger
from hdc.utils import file_parsers


def build_parser():
    parser = ArgumentParser(prog="hdc")

    parser.add_argument("-c", "--app_config", type=str, help="Path to application config (YAML) file if other "
                                                             "than default")
    parser.add_argument("-l", "--log_settings", type=str, help="Path to log settings (YAML) file if other than default")
    parser.add_argument("-r", "--run", type=str, required=True, choices=['catalog', 'map'], help="One of 'catalog' or "
                                                                                                 "'map'")
    parser.add_argument("-s", "--source", type=str, required=True, help="Name of any one of crawlers configured in "
                                                                        "app_config.yml")
    parser.add_argument("-d", "--destination", type=str,
                        help="Name of any one of creators configured in app_config.yml")
    parser.add_argument("-m", "--mapper", help="Name of any one of mappers configured in app_config.yml")

    return parser


def validate_hdc_cli_args(args):
    if len(args) != 0:
        if args.run.lower() == 'map':
            if args.destination is None:
                raise RuntimeError("For 'map' operation, destination option needs to be provided")
            if args.mapper is None:
                raise RuntimeError("For 'map' operation, mapper option needs to be provided")
    else:
        raise RuntimeError('No arguments provided!')


def get_default_app_config_path():
    return Path.home() / '.hdc' / 'app_config.yml'

def get_default_log_config_path():
    return Path.home() / '.hdc' / 'log_settings.yml'


def get_app_config(app_config):
    # If app_config.yml is given at the CLI
    if app_config is not None:
        config_dict = file_parsers.yaml_parser(app_config)
    # Else read from the default location
    else:
        with open(get_default_app_config_path(), 'r') as default_app_config:
            config_dict = file_parsers.yaml_parser(default_app_config)

    return config_dict


if __name__ == '__main__':

    hdc_parser = build_parser()
    cli_args = hdc_parser.parse_args()
    app_config: dict = get_app_config(cli_args.app_config)

    if cli_args.log_settings is not None:
        logging.config.dictConfig(file_parsers.yaml_parser(yaml_file_path=cli_args.log_settings))
    else:
        logging.config.dictConfig(file_parsers.yaml_parser(yaml_file_path=get_default_log_config_path()))

    if app_config is not None:
        if cli_args.run.lower() == 'map':
            asset_mapper: AssetMapper = providah_pkg_factory.create(key='AssetMapper',
                                                                    configuration={
                                                                        'source': app_config['crawlers'][
                                                                            cli_args.source],
                                                                        'destination': app_config['creators'][
                                                                            cli_args.destination],
                                                                        'mapper': app_config['mappers'][cli_args.mapper]})
            result = asset_mapper.map_assets()
        elif cli_args.run.lower() == 'catalog':
            cataloger: Cataloger = providah_pkg_factory.create(key='Cataloger',
                                                               configuration={
                                                                   'source': app_config['crawlers'][cli_args.source]})
            result = cataloger.obtain_catalog()

        else:
            raise RuntimeError("Unsupported option for 'run'")
    else:
        raise RuntimeError("Could not find app_config.yml; Please ensure it is available at the default location "
                           "else provide via CLI")

    pprint(result)
