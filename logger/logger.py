import logging.config
import os
import pathlib
from pathlib import Path

import colorlog
import yaml

from logger.helper import attach_document_logger


def color_console_handler():
    format_str = '%(asctime)s.%(msecs)d %(levelname)-4s [%(filename)s:%(lineno)d] %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    color_format = f'%(log_color)s{format_str}'
    colors = {'DEBUG': 'cyan',
              'INFO': 'green',
              'WARNING': 'bold_yellow',
              'ERROR': 'bold_red',
              'CRITICAL': 'bold_purple'}
    formatter = colorlog.ColoredFormatter(color_format, date_format,
                                          log_colors=colors)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    return stream_handler


def create_log_directory(log_file_path):
    return Path(log_file_path).resolve().mkdir(parents=True, exist_ok=True)


def load_logger_configuration(base_path, log_path):
    with open(os.path.join(base_path, 'logger.yaml'), 'r') as f:
        config = f.read().replace('{log_path}', str(log_path))
        config = yaml.safe_load(config)
        config['handlers']['file']['filename'] = str(pathlib.Path(log_path).joinpath('exec.log'))
        config['handlers']['error_log']['filename'] = str(pathlib.Path(log_path).joinpath('error.log'))
        config['handlers']['document_log']['filename'] = str(pathlib.Path(log_path).joinpath('error-document.log'))
        return config


def initialize_logger(mode, log_path, use_colors) -> logging.Logger:
    base_path = Path(__file__).parent
    logger = logging.getLogger(mode)
    attach_document_logger(logger)
    config = load_logger_configuration(base_path, log_path)
    create_log_directory(log_path)
    logging.config.dictConfig(config)
    if use_colors:
        logger.addHandler(color_console_handler())
    return logger
