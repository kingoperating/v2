## Developer: Gabe Tatman
## Date: 2024-02-28
## Description: This script will send out a daily report for a specific group of wells to a desired email distribution list


from dotenv import load_dotenv
from kingscripts.analytics import king
from kingscripts.operations import reporting
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

import os
import datetime as dt
import smtplib
import socket
import time
import os

# Getting Date Variables
dateToday = dt.datetime.today()
function = "daily_reporting"

# Load environment variables from .env file

load_dotenv()

# Email details
sender_email = "operations@kingoperating.com"
#receiver_email = os.getenv("GABE_TATMAN_EMAIL")
receiver_email = "mtanner@sandstone-group.com"
subject = "Successful Automation Run"
body = "The automation has run successfully."
# SMTP server details
smtp_server = "kingoperating-com.mail.protection.outlook.com"
port = 25

# Send email
reporting.send_script_confirmation_email(
    sender_email = sender_email, 
    receiver_email = receiver_email, 
    subject =   subject, 
    body = body, 
    smtp_server = smtp_server, 
    port = port
    )


dateEnd = dt.datetime.today()

runtimeSeconds = (dateEnd - dateToday).total_seconds()

usageFunction = king.updateUsageStatsEtlRuntime(
    etlStartTime=dateToday,
    etlEndTime=dateEnd,
    function=function,
    runtime=runtimeSeconds
)

