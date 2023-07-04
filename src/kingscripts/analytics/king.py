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
import pandas as pd


"""

Send Email Function - using production@kingoperating.com as the email sender

"""


def sendEmail(emailRecipient, emailRecipientName, emailSubject, emailMessage, nameOfFile, attachment=None):

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
            f"attachment; filename=" + nameOfFile + ".xlsx",
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
        print("Email sent successfully to " +
              emailRecipientName + "")
        server.quit()

    except:
        print("SMPT server connection error")

    return True


"""

Get Oil, Gas and BOE Averagy Daily Volumes and return them as a dataframe

"""


def getAverageDailyVolumes(masterKingProdData, startDate, endDate):

    # convert the dates to datetime
    masterKingProdData["Date"] = pd.to_datetime(masterKingProdData["Date"])
    masterKingProdData = masterKingProdData.sort_values(
        by=["Date"])

    startDate = pd.to_datetime(startDate)
    endDate = pd.to_datetime(endDate)

    durationBetweenDates = endDate - startDate
    daysBetweenDates = durationBetweenDates.days

    # Filter the data by date range
    masterKingProdData = masterKingProdData[(masterKingProdData["Date"] >= startDate) & (
        masterKingProdData["Date"] <= endDate)]

    masterKingProdData = masterKingProdData.reset_index()
    masterKingProdData = masterKingProdData.drop(columns=["index"])

    for i in range(0, len(masterKingProdData)):
        row = masterKingProdData.iloc[i]
        name = row["Well Accounting Name"]
        if name == "Read 332H" or name == "Read 342H":
            indexReplace = i
            oilVolume = row["Oil Volume"]
            netOilVolume = oilVolume * 0.5
            gasVolume = row["Gas Volume"]
            netGasVolume = 0
            masterKingProdData.at[indexReplace, "Oil Volume"] = netOilVolume
            masterKingProdData.at[indexReplace, "Gas Volume"] = netGasVolume
            x = 5

    # Get the average daily volumes for each column
    oilAvgDaily = masterKingProdData["Oil Volume"].sum() / daysBetweenDates
    gasAvgDaily = masterKingProdData["Gas Volume"].sum() / daysBetweenDates
    boeAvgDaily = oilAvgDaily + (gasAvgDaily / 6)

    # Create a dictionary
    avgDailyVolumes = {
        "Oil (Bbls/day)": [oilAvgDaily],
        "Gas (Mcf/day)": [gasAvgDaily],
        "BOE/day": [boeAvgDaily]
    }

    # Create a dataframe
    avgDailyVolumes = pd.DataFrame(avgDailyVolumes)

    return avgDailyVolumes
