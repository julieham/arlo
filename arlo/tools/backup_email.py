import mimetypes
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from parameters.credentials import login_gmail
from parameters.param import data_directory
from read_write.file_manager import get_last_update_string
from tools.logging import error


def send_email_backup_data(body='Hi Arlo, here is your data.', subject=' Refresh backup'):

    sender = login_gmail.username
    passwd = login_gmail.password

    msg = MIMEMultipart()
    name = get_last_update_string().replace(' ', '_')
    msg['From'] = sender
    msg['To'] = sender
    msg['Subject'] = subject
    msg.attach(MIMEText(body))

    for filename in ['data', 'provisions']:
        attachment = data_directory + filename + '.csv'
        ctype, _ = mimetypes.guess_type(attachment)
        maintype, subtype = ctype.split("/", 1)
        fp = open(attachment, "rb")
        attachment = MIMEBase(maintype, subtype)
        attachment.set_payload(fp.read())
        fp.close()
        encoders.encode_base64(attachment)
        attachment.add_header("Content-Disposition", "attachment", filename=filename + '_' + name + '.csv')
        msg.attach(attachment)


    mailserver = smtplib.SMTP('smtp.gmail.com', 587)
    try:
        mailserver.ehlo()
        mailserver.starttls()
        mailserver.ehlo()
        mailserver.login(sender, passwd)
        mailserver.sendmail(sender, sender, msg.as_string())
        mailserver.quit()
    except smtplib.SMTPAuthenticationError:
        error('#backup_mail authentication failed, refreshing anyway')


def save_backup_with_data():
    name = get_last_update_string().replace(' ', '_')
    for filename in ['data', 'provisions']:
        with open(data_directory + filename + '.csv', 'r') as file:
            with open(data_directory + 'backup/' + filename + '-' + name + '.csv', "w+") as file_dest:
                for line in file.readlines():
                    file_dest.write(line)
