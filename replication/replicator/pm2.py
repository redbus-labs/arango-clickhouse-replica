import pathlib
import subprocess

import yaml


def get_config_path():
    project_root = pathlib.Path(__file__).parent.parent.parent.absolute()
    return str(project_root.joinpath('pm2.yaml'))


def generate_config_file():
    project_root = pathlib.Path(__file__).parent.parent.parent
    config_path = project_root.joinpath('ecosystem.yaml').absolute()
    config_file = open(str(config_path), 'r')
    configs = yaml.safe_load(config_file)
    config_file.close()
    interpreter_path = str(project_root.joinpath('venv', 'bin', 'python'))
    scripts_path = ['replication/producer/publisher.py', 'replication/consumer/loader.py']
    for index, config in enumerate(configs['apps']):
        config['cwd'] = str(project_root)
        config['script'] = scripts_path[index]
        config['interpreter'] = interpreter_path
    with open('pm2.yaml', 'w') as pm2_configs:
        yaml.dump(configs, pm2_configs, default_flow_style=False)
    return True


class PM2:

    def __init__(self, process, config):
        self.process = process
        self.config = config

    @staticmethod
    def _execute(cmd):
        try:
            subprocess.check_output(cmd, shell=True)
            return True
        except subprocess.CalledProcessError:
            return False

    def start(self):
        return self._execute(f'pm2 start {self.config} --only={self.process}')

    def stop(self):
        return self._execute(f'pm2 stop {self.config} --only={self.process}')

    def restart(self):
        return self._execute(f'pm2 restart {self.config} --only={self.process}')
