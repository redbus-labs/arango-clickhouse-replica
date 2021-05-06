import os
import sys
from pathlib import Path

from alert import mailer
from alert.helper import get_mail_client
from alert.mailer import MailConfig
from config import env, reader
from logger import logger
from util.helper import singleton

CONFIG = 'CONFIG'
ENV_LOADER = 'ENV_LOADER'
LOGGER = 'LOGGER'
SMTP_CLIENT = 'SMTP_CLIENT'


def env_variables_loader():
    env.load_env()


def prepare_config_loader():
    config_loader = reader.load_config()
    return config_loader


# noinspection PyShadowingNames
def prepare_logger(log_path=None, env='dev'):
    if not log_path:
        log_path = str(Path(os.path.realpath(sys.argv[0])).parent.joinpath('logs'))
    mode = 'prod' if env == 'prod' else 'dev'
    logging = logger.initialize_logger(mode, log_path, mode == 'dev')
    return logging


def prepare_smtp_client(config):
    return get_mail_client(config)


def prepare_optional_smtp_client(config):
    return get_mail_client(config) if config['enabled'] else mailer.Mailer(MailConfig())


@singleton
def get_basic_utilities():
    basic_utils = BasicUtils()
    basic_utils.add((CONFIG, None, None)).add((LOGGER, None, {'env': os.getenv('env')}))
    configs = basic_utils.get(CONFIG)
    alert_config = {**configs['alert']['smtp'], 'enabled': configs['alert']['enabled']} if 'smtp' in configs[
        'alert'] else {'enabled': configs['alert']['enabled']}
    basic_utils.add((SMTP_CLIENT, [alert_config], None))
    return basic_utils


class BasicUtils:
    util_map = {
        ENV_LOADER: env_variables_loader,
        CONFIG: prepare_config_loader,
        LOGGER: prepare_logger,
        SMTP_CLIENT: prepare_optional_smtp_client
    }

    def __init__(self):
        self.utilities = {}

    def add(self, util):
        util_name, args, kwargs = util
        args = args if args else ()
        kwargs = kwargs if kwargs else {}
        self.utilities[util_name] = BasicUtils.util_map[util_name](*args, **kwargs)
        return self

    def get(self, util):
        return self.utilities[util]

    def get_utils(self, utils):
        objs = []
        for util in utils:
            objs.append(self.utilities[util])
        return objs
