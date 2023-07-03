# Importing libraries
import datetime as dt
import re
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path


"""

Send Email Function - using production@kingoperating.com as the email sender

"""


def sendEmail(emailRecipient, emailRecipientName, emailSubject, emailMessage, attachment=None):

    load_dotenv()  # loads the .env file

    email_sender = os.getenv("USERNAME_KING")
    msg = MIMEMultipart()
    msg["From"] = email_sender
    msg["To"] = emailRecipient
    msg["Subject"] = emailSubject
    msg.attach(MIMEText(emailMessage, "plain"))
    if attachment != None:
        # OPENS EACH ATTACHMENTS
        with open(attachment, "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            partTwo = MIMEBase("application", "octet-stream")
            partTwo.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(partTwo)

        partTwo.add_header(
            "Content-Disposition",
            f"attachment; filename= yesterdayWellReport.csv",
        )

        # ATTACHES EACH FILE TO EMAIL

        msg.attach(partTwo)

    text = msg.as_string()

    try:
        server = smtplib.SMTP("smtp.office365.com", 587)
        server.ehlo()
        server.starttls()
        server.login(os.getenv("USERNAME_KING"),
                     os.getenv("PASSWORD_KING"))
        server.sendmail(email_sender, emailRecipient, text)
        print("Bad Pumper Email sent successfully to " +
              emailRecipientName + "")
        server.quit()

    except:
        print("SMPT server connection error")

    return True
