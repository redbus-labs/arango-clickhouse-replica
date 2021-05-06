from util.basic_utils import get_basic_utilities, CONFIG


def get_supported_consumers():
    utils = get_basic_utilities()
    config = utils.get(CONFIG)
    collections = config['producer']['sync']
    exclude = config['consumer']['exclude']
    return [collection for collection in collections if collection not in exclude]


def get_supported_producers():
    utils = get_basic_utilities()
    config = utils.get(CONFIG)
    collections = config['producer']['sync']
    return collections
