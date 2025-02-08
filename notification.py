import smtplib
import credentials
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr


def send_email(subject, body):
    try:
        msg = MIMEMultipart()
        msg['From'] = formataddr(('Shoppalyzer Messenger', credentials.MAILFROM))
        msg['To'] = credentials.MAILTO
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(credentials.SMTP_SERVER, credentials.SMTP_PORT) as server:
            server.starttls()
            server.login(credentials.SMTP_USER, credentials.SMTP_PASSWORD)
            server.sendmail(credentials.MAILFROM, credentials.MAILTO, msg.as_string())
            print('Success notification sent.')
    except Exception as e:
        print(f'Error sending email: {e}')