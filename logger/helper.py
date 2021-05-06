import logging

DOCUMENT_LOGGER = 70


def attach_logger(logger: logging, msg, *args, **kwargs):
    if logger.isEnabledFor(DOCUMENT_LOGGER):
        # noinspection PyProtectedMember
        logger._log(DOCUMENT_LOGGER, msg, args, kwargs)


def attach_document_logger(logger: logging) -> logging:
    logging.addLevelName(DOCUMENT_LOGGER, "DOCUMENT")
    logging.document = attach_logger
    logging.Logger.document = attach_logger
    return logger
