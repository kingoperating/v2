## Developer: Gabe Tatman
## Date: 2024-02-28
## Description: This script will send out a daily report for a specific group of wells to a desired email distribution list

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import socket
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file

load_dotenv()

# Email details
sender_email = "operations@kingoperating.com"
receiver_email = os.getenv("GABE_TATMAN_EMAIL")
subject = "Test Email"
body = "This is a test email sent using Python."

def send_email(sender_email, receiver_email, subject, body, smtp_server, port):
    # Constructing the email
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))

    # Connect to the SMTP server without encryption
    server = smtplib.SMTP(smtp_server, port)
    server.set_debuglevel(1)  # Optional: Set debug level for more verbose output

    try:
        # Send the email without authentication
        server.sendmail(sender_email, receiver_email, message.as_string())
        print("Email sent successfully!")
    except smtplib.SMTPException as e:
        print(f"Failed to send email. Error: {str(e)}")
    finally:
        # Close the connection
        server.quit()


# SMTP server details
smtp_server = "kingoperating-com.mail.protection.outlook.com"
port = 25

# Send email
send_email(sender_email, receiver_email, subject, body, smtp_server, port)