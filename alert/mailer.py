import asyncio
import smtplib
from collections import namedtuple
from email.mime.multipart import MIMEMultipart

from asgiref.sync import sync_to_async

MailConfig = namedtuple('MailConfig', ['host', 'port', 'username', 'password', 'enabled'], defaults=(None,) * 5)


class Mailer:

    def __init__(self, config: MailConfig):
        self._server = None
        self.config = config

    def connect(self):
        self._server: smtplib.SMTP = smtplib.SMTP(self.config.host, self.config.port)
        self._server.ehlo()
        self._server.starttls()
        self._server.ehlo()
        self._server.login(self.config.username, self.config.password)

    def send(self, source, to, subject, cc=None, attachments=None):
        if not self.config.enabled:
            return False
        if cc is None:
            cc = []
        if attachments is None:
            attachments = []
        msg = MIMEMultipart('alternative')
        msg['From'] = source
        msg['To'] = ", ".join(to)
        msg['Cc'] = ", ".join(cc)
        msg['Subject'] = subject
        for attachment in attachments:
            msg.attach(attachment)
        return self._server.sendmail(source, to + cc, msg.as_string())

    def send_messages(self, mails, limit=1):

        if not self.config.enabled:
            return False

        async def async_mail(req):
            try:
                server = Mailer(self.config)
                server.connect()
                return await sync_to_async(server.send)(req['from'], req['to'], req['subject'], req['cc'],
                                                        req['attachments'])
            except Exception as e:
                return e

        async def main(reqs):
            return await asyncio.gather(*(async_mail(req) for req in reqs))

        result = []
        processed = 0
        while processed < len(mails):
            sub_set = mails[processed:processed + limit]
            result.extend(asyncio.run(main(sub_set)))
            processed += limit
        return result
