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
from pathlib import Path
import pandas as pd
from kingscripts.analytics import tech
import xlrd as xl

# load .env file
load_dotenv()

kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATAWAREHOUSE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))
kingWellsDatabase = str(os.getenv('SQL_WELLS_DATABASE'))
kingUsageDatabase = str(os.getenv('SQL_USAGE_DATABASE'))



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


def getNotReportedPumperList(masterKingProdData, checkDate):

    checkDate = datetime.strptime(checkDate, "%Y-%m-%d")

    masterWellsList = pd.read_excel(
        r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\master\masterWellAllocation.xlsx")

    pumperNames = masterWellsList["Pumper Name"].to_list()
    pumperNumbers = masterWellsList["Pumper Phone"].to_list()
    masterApiList = masterWellsList["API"].to_list()

    # Get the current date and sort the data by date
    masterKingProdData["Date"] = pd.to_datetime(masterKingProdData["Date"])
    masterKingProdData = masterKingProdData.sort_values(["Date"])

    # Calculate Rolling Averge 14-day window for Oil and Gas
    masterKingProdData["Rolling 14 Day Oil Average"] = masterKingProdData.groupby(
        "Well Accounting Name")["Oil Volume"].transform(lambda x: x.rolling(14, 1).mean())
    masterKingProdData["Rolling 14 Day Gas Average"] = masterKingProdData.groupby(
        "Well Accounting Name")["Gas Volume"].transform(lambda x: x.rolling(14, 1).mean())

    dataOnCheckDate = masterKingProdData[masterKingProdData["Date"] == checkDate]

    pumperNaughtyList = []

    for i in range(0, len(dataOnCheckDate)):
        row = dataOnCheckDate.iloc[i]
        oilVolume = row["Oil Volume"]
        gasVolume = row["Gas Volume"]
        rollingOilAverage = row["Rolling 14 Day Oil Average"]
        rollingGasAverage = row["Rolling 14 Day Gas Average"]
        date = row["Date"]

        if (oilVolume == 0 and rollingOilAverage > 0) or (gasVolume == 0 and rollingGasAverage > 0):
            wellName = row["Well Accounting Name"]
            api = row["API"]
            ## handle the case where the api is not in the master list aka returns a blank row or something
            if api not in masterApiList:
                continue
            else:
                indexOfApi = masterApiList.index(api)
                pumperName = pumperNames[indexOfApi]
                pumperNumber = pumperNumbers[indexOfApi]
                rollingOilAverage = round(rollingOilAverage, 2)
                rollingGasAverage = round(rollingGasAverage, 2)
                pumperNaughtyList.append([wellName, pumperName, pumperNumber, rollingOilAverage, rollingGasAverage])
        
    pumperNaughtyList = pd.DataFrame(pumperNaughtyList, columns=[
                                        "Well Name", "Pumper Name", "Pumper Number", "Rolling 14 Day Oil Average", "Rolling 14 Day Gas Average"])


    pumperNaughtyListName = pumperNaughtyList["Well Name"].tolist()
    
    oilDate = []
    gasDate = []
    
    for i in range (0, len(pumperNaughtyListName)):
        wellName = pumperNaughtyListName[i]
        trimmedWellData = masterKingProdData[masterKingProdData["Well Accounting Name"] == wellName]
        lastNonZeroOilData = trimmedWellData[trimmedWellData["Oil Volume"] != 0]
        lastNonZeroGasData = trimmedWellData[trimmedWellData["Gas Volume"] != 0]
        
        lastNonZeroOilDate = lastNonZeroOilData["Date"].max()
        lastNonZeroGasDate = lastNonZeroGasData["Date"].max()
        oilDate.append(lastNonZeroOilDate)
        gasDate.append(lastNonZeroGasDate)
    
    pumperNaughtyList["Last Non-Zero Oil Date"] = oilDate
    pumperNaughtyList["Last Non-Zero Gas Date"] = gasDate
    
    pumperNaughtyList["Last Non-Zero Oil Date"] = pd.to_datetime(pumperNaughtyList["Last Non-Zero Oil Date"])
    pumperNaughtyList["Last Non-Zero Gas Date"] = pd.to_datetime(pumperNaughtyList["Last Non-Zero Gas Date"])
    
        
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



"""
    
Reading Howard County Excel file and storing it in pandas dataframe

"""  

def getHCEFProduction(pathToFolder):
    
    headers = [
        "Date",
        "Oil",
        "Oil Sold",
        "Water",
        "Gas",
        "Comments"
        
    ]
    
    # create list of all files in pathToFolder
    files = os.listdir(pathToFolder)
    # get the most recent file
    files.sort(key=lambda x: os.path.getmtime(os.path.join(pathToFolder, x)))
    # create path to most recent file
    pathToData = os.path.join(pathToFolder, files[-1])
    # read the data
    readData = pd.read_excel(pathToData, header=2)

    readData = readData.drop(readData.columns[3], axis=1)
    readData = readData.drop(readData.columns[3], axis=1)
    readData = readData.drop(readData.columns[4], axis=1)
    readData = readData.drop(readData.columns[5], axis=1)
    readData = readData.drop(readData.columns[5:], axis=1)
    readData = readData.iloc[:7]
    ##insert new column
    readData.insert(5, "Comments", "None")
    # replace headers
    readData.columns = headers
    
    return readData

"""
    
Get Wyoming Data

"""  


def getWorlandUnit108Production(numberOfDays):
    
    headers = [
        "Date",
        "Oil",
        "Oil Sold",
        "Water",
        "Gas",
        "Comments"
    
    ]
    
    
    wu108Data = pd.read_excel(r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\production\wyoming\wu108Prod.xlsx", header=3)
    # replace NaN values with 0
    wu108Data = wu108Data.fillna(0)
    wu108DataLastTwoRows = wu108Data.tail(numberOfDays)
    wu108DataLastTwoRows = wu108DataLastTwoRows.drop(columns=["Cum oil prod bo"])
    wu108DataLastTwoRows = wu108DataLastTwoRows.drop(columns=["Cum gas prod mcf"])
    wu108DataLastTwoRows = wu108DataLastTwoRows.drop(columns=["Cum water prod bbls"])
    wu108DataLastTwoRows = wu108DataLastTwoRows.drop(columns=["Load left to recover bbls"])
    wu108DataLastTwoRows = wu108DataLastTwoRows.drop(columns=["% load recovery"])
    wu108DataLastTwoRows = wu108DataLastTwoRows.drop(columns=["Total Load to Recover bbls"])
    wu108DataLastTwoRows = wu108DataLastTwoRows.drop(columns=["Load added bbls"])
    wu108DataLastTwoRows = wu108DataLastTwoRows.drop(columns=["King Completions Comments"]) 
    wu108DataLastTwoRows = wu108DataLastTwoRows.drop(columns=["Report date"])
    wu108DataLastTwoRows = wu108DataLastTwoRows.drop(columns=["Gauge date"])
    
    ## convert the dates to datetime
    wu108DataLastTwoRows["Prod date"] = pd.to_datetime(wu108DataLastTwoRows["Prod date"])
    
    ## insert oil sold column at end
    wu108DataLastTwoRows.insert(5, "Oil Sold", 0)

    # order columns for header
    wu108DataLastTwoRows = wu108DataLastTwoRows[["Prod date", "Prod oil bo", "Oil Sold", "Prod water bwp", "Prod gas mcf", "Kentex Production Comments"]]
   
    # replace headers
    wu108DataLastTwoRows.columns = headers
    
    
    return wu108DataLastTwoRows


"""
    
Get Buffalo 6-8H Data from Power Automate folder

"""  

def getBuffalo68h(pathToFolder):
     
    headers = [
        "Date",
        "Oil",
        "Oil Sold",
        "Water",
        "Gas",
        "Comments"
        
    ]
    
    # create list of all files in pathToFolder
    files = os.listdir(pathToFolder)
    # get the most recent file
    files.sort(key=lambda x: os.path.getmtime(os.path.join(pathToFolder, x)))
    # create path to most recent file
    pathToData = os.path.join(pathToFolder, files[-1])
    # read the data
    buffData = pd.read_excel(pathToData, header=2)
    
    
    return buffData

"""
    
Getting Buffalo 6-8H data from powerAutomate folder\
    
"""  

def getBuffalo68h(pathToFolder):
    
    headers = [
        "Date",
        "Oil",
        "Oil Sold",
        "Water",
        "Gas",
        "Comments"
        
    ]
    
    # create list of all files in pathToFolder
    files = os.listdir(pathToFolder)
    # get the most recent file
    files.sort(key=lambda x: os.path.getmtime(os.path.join(pathToFolder, x)))
    # create path to most recent file
    pathToData = os.path.join(pathToFolder, files[-1])
    # read the data
    buffaloData = pd.read_excel(pathToData, engine="xlrd", header=3)
    
    ## convert Date column to pandas datetime
    buffaloData["Date"] = pd.to_datetime(buffaloData["Date"])
    ## sort by newest date to oldest date
    buffaloData = buffaloData.sort_values(by=["Date"])
    
    ## get the last 5 days of data
    buffaloData = buffaloData.tail(5)
    
    # drop all columns except for Date, BOPD, MCFD, BWPD and REMARKS
    buffaloData = buffaloData.drop(buffaloData.columns[5:20], axis=1)
    buffaloData = buffaloData.drop(buffaloData.columns[4], axis=1)
    
    ## replace all nan values with ""
    buffaloData = buffaloData.fillna("")
    
    ## insert oil sold column in between BOPD and MCFD
    buffaloData.insert(2, "Oil Sold", 0)
    
    ## switch MCFD and BWPD columns
    buffaloData = buffaloData[["Date", "BOPD", "Oil Sold", "BWPD", "MCFD", "REMARKS"]]
    
    ## replcace headers
    buffaloData.columns = headers
    
    ## rest index
    buffaloData = buffaloData.reset_index()
    
    ## convert date to string
    buffaloData["Date"] = buffaloData["Date"].dt.strftime("%m/%d/%Y")
    
    return buffaloData




"""
    
Usage Stats for the ETL

"""  

def updateUsageStatsEtl(etlStartTime):
    
    ## convert time to datetime
    etlStartTime = pd.to_datetime(etlStartTime)
    
    allocatedProduction = tech.getData(
        server=kingLiveServer,
        database= kingProductionDatabase,
        tableName= "allocated_production"
    )
    
    wellsData = tech.getData(
        server=kingLiveServer,
        database= kingWellsDatabase,
        tableName= "header_data"
    )
    
    dailyForecast = tech.getData(
        server=kingLiveServer,
        database= kingProductionDatabase,
        tableName= "daily_forecast"
    )
    
    lenAllocatedProduction = len(allocatedProduction)
    lenWellsData = len(wellsData)
    
    columnsUsageStats = [
        "Time",
        "Table",
        "Length"
    ]
    
    columnsUsageFunction = [
        "Time",
        "Function",
        "Runtime"
    ]

    ## create dataframe usage stats
    usageTableOne = pd.DataFrame(columns=columnsUsageStats)
    ## create row for usage dataframe
    usageRowOne = [etlStartTime, "allocated_production", int(lenAllocatedProduction)]
    usageRowTwo = [etlStartTime, "header_data", int(lenWellsData)]
    usageRowThree = [etlStartTime, "daily_forecast", int(len(dailyForecast))]
    ## append row to dataframe
    usageTableOne.loc[0] = usageRowOne
    usageTableOne.loc[1] = usageRowTwo
    usageTableOne.loc[2] = usageRowThree
    
    ## append into usage database
    tech.putDataAppend(
        server=kingLiveServer,
        database=kingUsageDatabase,
        data=usageTableOne,
        tableName="usage_stats"
    )
    
    
    return True

"""
    
Usage Stats etl runtime

"""  

def updateUsageStatsEtlRuntime(etlStartTime, etlEndTime, function, runtime):
    
    etlStartTime = pd.to_datetime(etlStartTime)
    runtime = int(runtime)
    
    ## create dataframe usage stats
    columnsUsageFunction = [
        "Time",
        "Function",
        "Runtime"
    ]
    
    usageTableTwo = pd.DataFrame(columns=columnsUsageFunction)
    ## create row for usage dataframe
    usageRowThree = [etlStartTime, function, runtime]
    ## append row to dataframe
    usageTableTwo.loc[0] = usageRowThree
    
    ## append into usage database
    tech.putDataAppend(
        server=kingLiveServer,
        database=kingUsageDatabase,
        data=usageTableTwo,
        tableName="usage_function"
    )
    
    return True