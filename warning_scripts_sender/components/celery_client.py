import base64
import logging
import os
import smtplib
import traceback
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from celery import Celery

root_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

logging.basicConfig(filename=root_path + '/warning_sender.log', filemode='a')

app = Celery('email_sender_warning',
             broker="redis://{}:{}/0".format(os.environ.get("redis_host"), os.environ.get("redis_port")))
print("connected celery")
smtp_server = os.environ.get("smtp_server")
smtp_port = os.environ.get("smtp_port")
sender_email = os.environ.get("sender_email")
email_password = os.environ.get("email_password")
image_base64 = "random_image_base_64format"


@app.task(name='components.celery_client.send_your_emai')
def send_your_email(content, email_recipient):
    """
    sends email to user, by queuing
    @params :content: txt, content of email should be sent
    @params :email_recipient: list,str, email of users
    """
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(sender_email, email_password)

    msg = MIMEMultipart()
    msg['Subject'] = 'Subject'
    msg['From'] = sender_email
    msg['To'] = email_recipient
    msg.attach(MIMEText(content))

    # Add image as an attachment
    image = MIMEImage(base64.b64decode(image_base64))
    image.add_header('Content-Disposition', 'attachment', filename='Logo.png')
    msg.attach(image)

    try:
        server.send_message(msg)
        print("email sent")
        del msg['To']
    except Exception as e:
        err = traceback.format_exc()
        logging.error(err)
    finally:
        server.quit()
