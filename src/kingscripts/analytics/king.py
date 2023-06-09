# Importing libraries
import datetime as dt
from datetime import datetime
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

Send Email Function - using operations@kingoperating.com as the email sender

"""


def sendEmail(emailRecipient, emailRecipientName, emailSubject, emailMessage, nameOfFile=None, attachment=None):

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


"""
Returns a List of Pumpers that have not submitted their daily reports

"""


def getNotReportedPumperList(masterAllocatedData, checkDate):

    checkDate = datetime.strptime(checkDate, "%Y-%m-%d")

    masterWellsList = pd.read_excel(
        r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\master\masterWellAllocation.xlsx")

    pumperNames = masterWellsList["Pumper Name"].to_list()
    pumperNumbers = masterWellsList["Pumper Phone"].to_list()
    masterApiList = masterWellsList["API"].to_list()

    # Get the current date and sort the data by date
    masterAllocatedData["Date"] = pd.to_datetime(masterAllocatedData["Date"])
    masterAllocatedData = masterAllocatedData.sort_values(["Date"])

    # Calculate Rolling Averge 14-day window for Oil and Gas
    masterAllocatedData["Rolling 14 Day Oil Average"] = masterAllocatedData.groupby(
        "Well Accounting Name")["Oil Volume"].transform(lambda x: x.rolling(14, 1).mean())
    masterAllocatedData["Rolling 14 Day Gas Average"] = masterAllocatedData.groupby(
        "Well Accounting Name")["Gas Volume"].transform(lambda x: x.rolling(14, 1).mean())

    dataOnCheckDate = masterAllocatedData[masterAllocatedData["Date"] == checkDate]

    pumperNaughtyList = []

    for i in range(0, len(dataOnCheckDate)):
        row = dataOnCheckDate.iloc[i]
        oilVolume = row["Oil Volume"]
        gasVolume = row["Gas Volume"]
        rollingOilAverage = row["Rolling 14 Day Oil Average"]
        rollingGasAverage = row["Rolling 14 Day Gas Average"]

        if (oilVolume == 0 and rollingOilAverage > 0) or (gasVolume == 0 and rollingGasAverage > 0):
            wellName = row["Well Accounting Name"]
            api = row["API"]
            indexOfApi = masterApiList.index(api)
            pumperName = pumperNames[indexOfApi]
            pumperNumber = pumperNumbers[indexOfApi]
            pumperNaughtyList.append([wellName, pumperName, pumperNumber])

    pumperNaughtyList = pd.DataFrame(pumperNaughtyList, columns=[
                                     "Well Name", "Pumper Name", "Pumper Number"])

    return pumperNaughtyList


"""

Creates the message that gets sent to pumpers - NOT SHARED IN DOCUMENTATION

"""


def createPumperMessage(badPumperData, badPumperTrimmedList, badPumperMessage):

    for i in range(0, len(badPumperTrimmedList)):
        pumperName = badPumperTrimmedList[i]
        indices = badPumperData[badPumperData["Pumper Name"]
                                == pumperName].index
        pumperPhoneNumber = badPumperData["Pumper Number"][indices].to_list()
        wellNames = badPumperData["Well Name"][indices].to_list()
        badPumperMessage = badPumperMessage + \
            "Pumper Name: " + str(pumperName) + " - Phone Number: " + \
            str(pumperPhoneNumber[0]) + "\n"
        badPumperMessage = badPumperMessage + "  Well Name(s): \n"

        for j in range(0, len(wellNames)):
            badPumperMessage = badPumperMessage + \
                "     " + str(wellNames[j]) + "\n"

        badPumperMessage = badPumperMessage + "\n"

    return badPumperMessage
