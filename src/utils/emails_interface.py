import requests
from requests.auth import HTTPBasicAuth
from src.utils.config import *
import logging
from typing import Union, List
import logging
logger = logging.getLogger('main_logger')


class MailGunEmail:
    def __init__(self, env):
        self.session = requests.session()
        self.session.auth = requests.auth.HTTPBasicAuth(username='api', password=MAILGUN_KEY)
        self.env = env

    def send_text_email(self, subject, text):
        try:
            data = {
                "from": "Tipstronic <hello@tipstronic.com>",
                "to": "### EMAIL ###",
                "subject": f'{self.env} - {subject}',
                "text": text
            }
            req = requests.Request(method='POST',
                                   url='https://api.eu.mailgun.net/v3/tipstronic.com/messages',
                                   data=data)
            prepped = self.session.prepare_request(req)
            response = self.session.send(prepped)
            if response.status_code != 200:
                logger.error(f'Could not send email with subject = {str(subject)} and text = {str(text)}')
            else:
                logger.info(f'Sent email with subject = {str(subject)} and text = {str(text)}')
        except:
            import traceback
            logger.error(traceback.format_exc())

    def send_email_with_attach(self, subject: str, text: str, to: str, attach_file_path: Union[str, List], file_name: Union[str, List]):
        '''
        Sends email with attach
        :param subject:
        :param text:
        :param to:
        :param attach_file_path:
        :return:
        '''
        try:
            data = {
                "from": "Tipstronic <hello@tipstronic.com>",
                "to": "### EMAIL ###",
                "subject": f'{self.env} - {subject}',
                "text": text
            }
            if isinstance(file_name, str):
                file_name = [file_name]
            if isinstance(attach_file_path, str):
                attach_file_path = [attach_file_path]
            files = [("attachment", (f, open(p, 'rb').read())) for f, p in zip(file_name, attach_file_path)]
            req = requests.Request(method='POST',
                                   url='https://api.eu.mailgun.net/v3/tipstronic.com/messages',
                                   data=data,
                                   files=files
                                   )
            prepped = self.session.prepare_request(req)
            response = self.session.send(prepped)
            if response.status_code != 200:
                logger.error(f'Could not send email with attach with subject = {str(subject)} and text = {str(text)}')
            else:
                logger.info(f'Sent email with attach with subject = {str(subject)} and text = {str(text)}')
        except:
            import traceback
            logger.error(traceback.format_exc())
