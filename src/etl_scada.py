"""
ETL for SCADA - KOC Python Packages 3.3.0

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

'''
FIRST - MAKE SURE ALL THE ENVIRONMENT VARIABLES ARE SET IN THE .ENV FILE

SECOND - ENSURE YOUR WORKING DATA DIRECTORY IS SET TO THE CORRECT FOLDER. CURRENT THIS SCRIPT REFERENCES THE KOC DATAWAREHOUSE V1.0.0

'''

# getting API keys
joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))

## SQL Server Variables - for KOC Datawarehouse 3.0
kingServerExpress = str(os.getenv('SQL_SERVER'))
kingDatabaseExpress = str(os.getenv('SQL_KING_DATABASE'))
kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATAWAREHOUSE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))

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
joynUser = str(os.getenv('JOYN_USER'))

## BEGIN ETL PROCESS

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

userId = joyn.getJoynUsers(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    nameToFind=joynUser
)

read332HData = king.getHCEFProduction(
    pathToFolder=pathToRead332H,
)

read342HData = king.getHCEFProduction(
    pathToFolder=pathToRead342H,
)

joyn.putJoynDataApi(
    userId=userId,
    rawProductionData=read332HData,
    objectId=read332hId,
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

joyn.putJoynDataApi(
    userId=userId,
    rawProductionData=read342HData,
    objectId=read342Id,
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

print("ETL Process Complete")