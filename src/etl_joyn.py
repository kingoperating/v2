"""
ETL Script for Production Data

Developer: Michael Tanner, Gabe Tatman

"""
# KOC v3.3.0 Python Packages
from kingscripts.operations import greasebook, combocurve, joyn
from kingscripts.analytics import enverus, king, tech
from kingscripts.finance import wenergy, afe

#  Needed Python Packages
from dotenv import load_dotenv
from combocurve_api_v1 import ServiceAccount
import os
import datetime as dt
from datetime import timedelta
import pandas as pd

# load .env file
load_dotenv()

# getting API keys
enverusApiKey = os.getenv('ENVERUS_API')
greasebookApi = os.getenv('GREASEBOOK_API_KEY')
#serviceAccount = ServiceAccount.from_file(
    #os.getenv("COMBOCURVE_API_SEC_CODE_LIVE"))
comboCurveApiKey = os.getenv("COMBOCURVE_API_KEY_PASS_LIVE")
joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))
function = "etl_joyn"

## SQL Server Variables - for KOC Datawarehouse 3.0
kingServerExpress = str(os.getenv('SQL_SERVER'))
kingDatabaseExpress = str(os.getenv('SQL_KING_DATABASE'))
kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATAWAREHOUSE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))
kingWellsDatabase = str(os.getenv('SQL_WELLS_DATABASE'))

# Getting Date Variables
dateToday = dt.datetime.today()
isTuseday = dateToday.weekday()
dateLastSunday = dateToday - timedelta(days=2)
dateEightDaysAgo = dateToday - timedelta(days=8)
dateYesterday = dateToday - timedelta(days=1)
todayYear = dateToday.strftime("%Y")
todayMonth = dateToday.strftime("%m")
todayDay = dateToday.strftime("%d")

yesYear = int(dateYesterday.strftime("%Y"))
yesMonth = int(dateYesterday.strftime("%m"))
yesDay = int(dateYesterday.strftime("%d"))
yesDateString = dateYesterday.strftime("%Y-%m-%d")
todayDateString = dateToday.strftime("%Y-%m-%d")
eightDayAgoString = dateEightDaysAgo.strftime("%Y-%m-%d")

# Important Variables for scripts
nglStream = 0
noNglStream = 1
texas = "texas"
wyoming = "wyoming"
browning5181H = "42033325890000"
browningOperatorName = "BROWNING OIL"
basin = "MIDLAND"
comboCurveProjectId = "612fc3d36880c20013a885df"
comboCurveScenarioId = "64aca0abaa25aa001201b299"
comboCurveForecastId = "64a5d95390c0320012a83df9"
millerranchb501mh = "millerranchb501mh"
millerrancha502v = "millerrancha502v"
millerrancha501mh = "millerrancha501mh"
millerranchc301 = "millerranchc301"
millerranchc302mh = "millerranchc302mh"
thurman23v = "thurman23v"
chunn923v = "chunn923v"
ayres79v = "ayres79v"
porter33v = "porter33v"
wu108 = "wu108"
wu105 = "wu105"
wu99 = "wu99"
kinga199cv1h = "kinga199cv1h"
kinga199cv2h = "kinga199cv2h"
nameOfWell = "thurman23v"
irvinSisters53m1h = "irvinsisters53m1h"
pshigoda752h = "pshigoda752h"
itSqlTable = "itSpend"
daysToPull = 35
daysToLookBack = 2
testFile = r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\assetPrettyName.xlsx"
listOfWells = [
    irvinSisters53m1h,
    pshigoda752h,
    thurman23v,
    chunn923v,
    ayres79v,
    porter33v,
    kinga199cv1h,
    kinga199cv2h,
    wu108,
    wu105,
    wu99,
    millerrancha501mh,
    millerrancha502v,
    millerranchb501mh,
    millerranchc301,
    millerranchc302mh
]

### BEGIN PRODUCTION ETL PROCESS ###

print("Beginning Production ETL Process")

# Get Well Header Data
wellData = joyn.getWellHeaderData(
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

# Append Well Header Data to SQL Server
tech.putDataReplace(
    server=kingLiveServer,
    database=kingWellsDatabase,
    data=wellData,
    tableName="header_data"
)

# Gets Historical Allocated Production from KOC Datawarehouse 3.0
historicalAllocatedProduction = tech.getData(
    server=kingLiveServer,
    database= kingProductionDatabase,
    tableName= "daily_production"
)

print("Length of Historical Allocated Production: " + str(len(historicalAllocatedProduction)))

# Get Last _ Days of Production From JOYN
joynProduction = joyn.getDailyAllocatedProductionRawWithDeleted(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    wellHeaderData=wellData,
    daysToLookBack=2
)

# Compare the two dataframes and get the duplicates
duplicatedIdList = joyn.compareJoynSqlDuplicates(
    joynData=joynProduction,
    sqlData=historicalAllocatedProduction
)

listOfIds = duplicatedIdList['UUID'].tolist()

# Delete duplicate from Historical Allocated Production
deleteRecords = tech.deleteDuplicateRecords(
    server=kingLiveServer,
    database= kingProductionDatabase,
    tableName="daily_production",
    duplicateList=listOfIds
)

# Get New Length of Historical Allocated Production
lengthOfWorkingTable = tech.getData(
    server=kingLiveServer,
    database= kingProductionDatabase,
    tableName="daily_production"
)

print("All Historical Record Length After Deleting: " + str(len(lengthOfWorkingTable)))

## Append joyn production to historical allocated production
tech.putDataAppend(
    server=kingLiveServer,
    database=kingProductionDatabase,
    data=joynProduction,
    tableName="daily_production"
)

historicalAllocatedProduction = tech.getData(
    server=kingLiveServer,
    database=kingProductionDatabase,
    tableName="daily_production"
)

print("All Historical Record Length After Appending: " + str(len(historicalAllocatedProduction)))

print("Genius")

    