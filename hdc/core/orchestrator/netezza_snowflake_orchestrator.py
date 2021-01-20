from hdc.core.orchestrator.orchestrator import Orchestrator
from hdc.factory.package_factory import PackageFactory


class NetezzaSnowflakeOrchestrator(Orchestrator):
    def run_cataloging(self):
        data_tuple = PackageFactory.catalog(self._source_env, self._config_path)
        if data_tuple:
            sql_tuple = PackageFactory.map(data_tuple)
            PackageFactory.write(self._destination_env, self._config_path, sql_tuple)
