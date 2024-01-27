"""
ETL for SCADA - KOC Python Packages 3.4.0

Developer: Michael Tanner, Gabe Tatman

"""
# KOC v3.4.0 Python Packages
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

'''
FIRST - MAKE SURE ALL THE ENVIRONMENT VARIABLES ARE SET IN THE .ENV FILE

SECOND - ENSURE YOUR WORKING DATA DIRECTORY IS SET TO THE CORRECT FOLDER. CURRENT THIS SCRIPT REFERENCES THE KOC DATAWAREHOUSE V1.0.0

'''

# getting API keys
joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))

## SQL Server Variables
kingDatabaseExpress = str(os.getenv('SQL_KING_DATABASE'))
kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATAWAREHOUSE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))
kingUsageDatabase = str(os.getenv('SQL_USAGE_DATABASE'))

## Names of KOC Employees
michaelTanner = os.getenv("MICHAEL_TANNER_EMAIL")
michaelTannerName = os.getenv("MICHAEL_TANNER_NAME")
gabeTatman = os.getenv("GABE_TATMAN_EMAIL")
gabeTatmanName = os.getenv("GABE_TATMAN_NAME")

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
pathToRead332H = str(os.getenv("READ_332H"))
pathToRead342H = str(os.getenv("READ_342H"))
pathToBuffalo68h = str(os.getenv("BUFFALO_68H"))
joynUser = str(os.getenv('JOYN_USER'))
function = "etl_scada"

## BEGIN ETL PROCESS

# Get object IDs for wells
read332hId = joyn.getWellObjectId(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    nameOfWell="Read 332H"
)

read342Id = joyn.getWellObjectId(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    nameOfWell="Read 342H"
)

buffalo68Id = joyn.getWellObjectId(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    nameOfWell="Buffalo 6-8 1H"
)

# Get User ID for Michael Tanner / Gabe Tatman

userId = joyn.getJoynUser(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    nameToFind=joynUser
)

# Get Production Data from SCADA folders

read332HData = king.getHCEFProduction(
    pathToFolder=pathToRead332H,
)

read342HData = king.getHCEFProduction(
    pathToFolder=pathToRead342H,
)

buffalo68hData = king.getBuffalo68h(
    pathToFolder=pathToBuffalo68h,
)

# Put Production Data in JOYN

joyn.putJoynData(
    userId=userId,
    rawData=read332HData,
    objectId=read332hId,
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

joyn.putJoynData(
    userId=userId,
    rawData=read342HData,
    objectId=read342Id,
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

joyn.putJoynData(
    userId=userId,
    rawData=buffalo68hData,
    objectId=buffalo68Id,
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

# USAGE STATS

dateEnd = dt.datetime.today()

runtimeSeconds = (dateEnd - dateToday).total_seconds()

usageFunction = king.updateUsageStatsEtlRuntime(
    etlStartTime=dateToday,
    etlEndTime=dateEnd,
    function=function,
    runtime=runtimeSeconds
)

print("ETL Process Complete")