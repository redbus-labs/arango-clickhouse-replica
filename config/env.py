import os
from pathlib import Path

from dotenv import load_dotenv

project_directory = Path(__file__).parent.parent


def load_variables(variables):
    for name, value in variables.items():
        os.environ[name] = value


def load_env():
    env_path = f'{project_directory}/config/.env'
    load_dotenv(dotenv_path=env_path)
