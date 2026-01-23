import airbyte as ab
import subprocess


def override_module_version(modules_dict, connector_name='source-s3'):
    try:
        python_bin = f"/opt/dagster/app/.venv-{connector_name}/bin/python"
        for module_name_with_version in modules_dict.items():
            module_name, version = module_name_with_version
            pip_install = subprocess.Popen([python_bin, '-m', 'pip', 'install', f'{module_name}=={version}'])
            pip_install.wait()
    except Exception:
        pass


class AirbyteAsset:
    def __init__(self, connector_type, config):
        self.connector_type = connector_type
        self.config = config
        self.source = None


class AirbyteAPIBasedAsset(AirbyteAsset):
    def __init__(self, connector_type, config):
        super().__init__(connector_type, config)
        self.get_or_create()

    def get_or_create(self):
        pass

    def read_streams(self, streams):
        pass


class PyAirbyteBasedAsset(AirbyteAsset):
    def __init__(self, connector_type, config):
        super().__init__(connector_type, config)
        self.get_or_create()

    def get_or_create(self):
        self.source = ab.get_source(
             self.connector_type,
             install_if_missing=True,
             config=self.config
        )

    def read_streams(self, streams, cache=None):
        #Call explicit pip installation if some libraries are missing (or there are new corrupted versions)
        override_module_version({'pendulum': '3.0.0'})
        override_module_version({'airbyte-source-s3': '4.11.0'})
        #Read using connector
        if cache:
            read_result: ab.ReadResult = self.source.read(streams=streams, cache=cache)
        else:
            read_result: ab.ReadResult = self.source.read(streams=streams)
        return read_result
