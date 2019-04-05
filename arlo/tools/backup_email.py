import mimetypes
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from parameters.credentials import login_gmail
from read_write.file_manager import get_last_update_string


def send_email_backup_data(body='Hi Arlo, here is your data.'):

    sender = login_gmail.username
    passwd = login_gmail.password

    msg = MIMEMultipart()
    name = get_last_update_string().replace(' ', '_')
    msg['From'] = sender
    msg['To'] = sender
    msg.attach(MIMEText(body))

    attachment = './arlo/data/data.csv'
    ctype, _ = mimetypes.guess_type(attachment)
    maintype, subtype = ctype.split("/", 1)
    fp = open(attachment, "rb")
    attachment = MIMEBase(maintype, subtype)
    attachment.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(attachment)
    attachment.add_header("Content-Disposition", "attachment", filename=name + '.csv')
    msg.attach(attachment)

    mailserver = smtplib.SMTP('smtp.gmail.com', 587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login(sender, passwd)
    mailserver.sendmail(sender, sender, msg.as_string())
    mailserver.quit()
