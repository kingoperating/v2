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
import pdfplumber

# load .env file
load_dotenv()

kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATAWAREHOUSE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))
kingWellsDatabase = str(os.getenv('SQL_WELLS_DATABASE'))
kingUsageDatabase = str(os.getenv('SQL_USAGE_DATABASE'))
kingTannerUsername = str(os.getenv('SQL_SERVER_MICHAEL_TANNER_USERNAME'))
kingTannerPassword = str(os.getenv('SQL_SERVER_MICHAEL_TANNER_PASSWORD'))



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
    readData = pd.read_excel(pathToData, header=3)

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
    
Get Browning Non-Op Data

"""  

def getBrowningProduction(pathToData, wellName):
    
    headers = [
        "Date",
        "Oil",
        "Oil Sold",
        "Water",
        "Gas",
        "Comments"
    ]
    
    # create list of all files in pathToFolder
    files = os.listdir(pathToData)
    # get the most recent file
    files.sort(key=lambda x: os.path.getmtime(os.path.join(pathToData, x)))
    # create path to most recent file
    pathToData = os.path.join(pathToData, files[-1])
    
    
    try:
        # open pdf file
        with pdfplumber.open(pathToData) as pdf:
            # get the first page
            page = pdf.pages[0]
            # extract the table
            table = page.extract_table()
            
            if not table:
                return None, "No table found in the PDF."
         
            # The first row is the header
            headers = table[0]
            data_rows = table[1:]
            
            # Define expected column names based on provided data
            expected_headers = ['Date', 'Well Name', 'API #', 'PIP', 
                              'Oil Production', 'Gas Production', 
                              'Water Production', 'Comments']
            
            if len(headers) != len(expected_headers):
                return None, f"Table header mismatch. Expected {expected_headers}, got {headers}"
            
            # Create DataFrame from table
            df = pd.DataFrame(data_rows, columns=headers)
            
            # Filter for the specified well name (case-insensitive)
            filtered_df = df[df['Well Name'].str.contains(wellName, case=False, na=False)]

            if filtered_df.empty:
                return None, f"No data found for well name: {wellName}"
            
            # Create the output DataFrame with requested columns
            output_df = pd.DataFrame({
                'Date': filtered_df['Date'],
                'Oil': filtered_df['Oil Production'],
                'Oil Sold': filtered_df['Oil Production'],  # Oil Sold is same as Oil
                'Water': filtered_df['Water Production'],
                'Gas': filtered_df['Gas Production'],
                'Comments': filtered_df['Comments']
            })
            
            return output_df, None
    
    except FileNotFoundError:
        return None, f"PDF file not found at: {pathToData}"
    except Exception as e:
        return None, f"Error processing PDF: {str(e)}"  


"""
    
Get Wyoming Data

"""  

def getWorlandUnit108Production(pathToFolder):
    
    headers = [
        "Date",
        "Oil",
        "Oil Sold",
        "Water",
        "Gas",
        "Comments"
    ]
    
    # create list of all files in pathToFolder
    files = [f for f in os.listdir(pathToFolder) if f.endswith('.csv')]
    # get the most recent file
    files.sort(key=lambda x: os.path.getmtime(os.path.join(pathToFolder, x)))
    # create path to most recent file
    pathToData = os.path.join(pathToFolder, files[-1])
    
    wu108Data = pd.read_csv(pathToData)
    
    ## drop first column
    wu108Data = wu108Data.drop(wu108Data.columns[0], axis=1)
    
    ## switch 3 and 4 columns by index
    wu108Data = wu108Data[[wu108Data.columns[0], wu108Data.columns[1], wu108Data.columns[2], wu108Data.columns[4], wu108Data.columns[3], wu108Data.columns[5]]]
    
    wu108Data.columns = headers
    
    ## conver to datetime
    wu108Data["Date"] = pd.to_datetime(wu108Data["Date"])
    ## convert to string
    wu108Data["Date"] = wu108Data["Date"].dt.strftime("%m/%d/%Y")
    
    return wu108Data


"""
    
Getting Buffalo 6-8H data from powerAutomate folder\
    
"""  

def getChollaData(pathToFolder, daysToLookback, sheetName = None):
    
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
    buffaloData = pd.read_excel(pathToData, engine="xlrd", header=3, sheet_name=sheetName)
    
    ## convert Date column to pandas datetime
    buffaloData["Date"] = pd.to_datetime(buffaloData["Date"])
    ## sort by newest date to oldest date
    buffaloData = buffaloData.sort_values(by=["Date"])
    
    ## get the last 5 days of data
    buffaloData = buffaloData.tail(daysToLookback)
    
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

## Import ConocoPhillips Data into the King Datawarehouse

def getConocoEchoUnit(pathToFolder, daysToLookBack):
    
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
    
    echoUnitData = pd.read_csv(pathToData)
    
    ##convert PRODDATE to datetime
    echoUnitData["PRODDATE"] = pd.to_datetime(echoUnitData["PRODDATE"], format="mixed")
    echoUnitData = echoUnitData.sort_values(by=["PRODDATE"], ascending=False)
    
    ##get last 10 days of data
    echoUnitData = echoUnitData.head(daysToLookBack)
    
    ## drop WELL ID, COMPLETION NO, WELL NAME, API, GAS SALES, TUBING PRESSURE, CASING PRESSURE, BOTTOMHOLE PRESSURE
    headersToDrop = [
        "WELL ID",
        "COMPLETION NO",
        "WELL NAME",
        "API",
        "GAS SALES",
        "TUBING PRESSURE",
        "CASING PRESSURE",
        "BOTTOMHOLE PRESSURE",
    ]
    
    echoUnitData = echoUnitData.drop(columns=headersToDrop)

    
    ## insert comments column with ""
    echoUnitData.insert(5, "Comments", "")

    # Swap columns 'GAS PROD' and 'WATER PROD' to maintain correct upload order
    echoUnitData['GAS PROD'], echoUnitData['WATER PROD'] = echoUnitData['WATER PROD'], echoUnitData['GAS PROD']
    
    ## replace headers
    echoUnitData.columns = headers
    ## convert date to string
    echoUnitData["Date"] = echoUnitData["Date"].dt.strftime("%m/%d/%Y")
    ## convert nan to ""
    echoUnitData = echoUnitData.fillna("")
    
    return echoUnitData

"""
    
Usage Stats for the ETL

"""  

def updateUsageStatsEtl(etlStartTime):
    
    ## convert time to datetime
    etlStartTime = pd.to_datetime(etlStartTime)
    
    allocatedProduction = tech.getData(
        server=kingLiveServer,
        database= kingProductionDatabase,
        tableName= "daily_production"
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
    
    dailyWellReading = tech.getData(
        server=kingLiveServer,
        database= kingProductionDatabase,
        tableName= "daily_well_reading"
    )
    
    lenAllocatedProduction = len(allocatedProduction)
    lenWellsData = len(wellsData)
    lenDailyForecast = len(dailyForecast)
    lenDailyWellReading = len(dailyWellReading)
    
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
    usageRowThree = [etlStartTime, "daily_forecast", int(lenDailyForecast)]
    usageRowFour = [etlStartTime, "daily_well_reading", int(lenDailyWellReading)]
    ## append row to dataframe
    usageTableOne.loc[0] = usageRowOne
    usageTableOne.loc[1] = usageRowTwo
    usageTableOne.loc[2] = usageRowThree
    usageTableOne.loc[3] = usageRowFour
    
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
        tableName="usage_function",
        uid=kingTannerUsername,
        password=kingTannerPassword
        
    )
    
    return True