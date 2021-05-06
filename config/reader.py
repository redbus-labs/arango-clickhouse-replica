import os
from pathlib import Path

import yaml


def load_config():
    base_path = Path(__file__).parent.parent
    file_path = os.path.join(base_path, f'settings.yaml')
    if file_path:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    raise FileNotFoundError
