# -*- coding:utf-8 -*-

import logging
import smtplib
from email.mime.text import MIMEText


class MailBoxHandler(logging.Handler):
    """
    Mail Box for sending emails
    """

    def __init__(self, level=logging.NOTSET):
        logging.Handler.__init__(self, level)
        self.mailto_list = ['batnos@163.com']
        self.mail_host = "smtp.163.com"  # 设置服务器
        self.mail_user = "batnos"  # 用户名
        self.mail_pass = "batnosbatnos2016"  # 口令
        self.mail_postfix = "163.com"  # 发件箱的后缀

    def emit(self, record):
        try:
            if record.name == 'qq_logger':
                self.send_mail('QQ Crawler Msg', record.msg)
        except Exception as ex:
            return

    def send_mail(self, sub, content):
        """
        Send mail to tolist with subject sub and content content
        :param to_list:
        :param sub:
        :param content:
        :return:
        """
        me = self.mail_user + "@" + self.mail_postfix
        msg = MIMEText(content, _subtype='plain', _charset='gb2312')
        msg['Subject'] = sub
        msg['From'] = me
        msg['To'] = ";".join(self.mailto_list)
        try:
            server = smtplib.SMTP()
            server.connect(self.mail_host)
            server.login(self.mail_user, self.mail_pass)
            server.sendmail(me, self.mailto_list, msg.as_string())
            server.close()
            return True
        except Exception as ex:
            print(ex)
            return False


if __name__ == '__main__':
    print('hello')
