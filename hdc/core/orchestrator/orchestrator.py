class Orchestrator:
    def __init__(self, **kwargs):
        self._source_env = kwargs.get('source_env')
        self._destination_env = kwargs.get('destination_env')
        self._config_path = kwargs.get('config_path')

    def run_cataloging(self):
        raise NotImplementedError(f'Method not implemented for {type(self).__name__}.')