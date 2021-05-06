from email import encoders
from email.mime.base import MIMEBase

from util.helper import singleton
from . import mailer
from .mailer import MailConfig


def prepare_attachment(data, name=''):
    payload = MIMEBase('application', 'octet-stream')
    payload.set_payload(data)
    encoders.encode_base64(payload)
    payload.add_header('Content-Disposition', f'attachment; filename= {name}')
    return payload


def get_mail_client(mail_config):
    mail = mailer.Mailer(MailConfig(
        host=mail_config['host'],
        port=mail_config['port'],
        username=mail_config['user'],
        password=mail_config['password'],
        enabled=mail_config['enabled']
    ))
    mail.connect()
    return mail


@singleton
def get_singleton_mail_client(mail_config):
    """
     get singleton mail client
     with this function only one instance can be crated through out the program
     manually create the objects using singleton func to create multiple singletons
    """
    return get_mail_client(mail_config)
