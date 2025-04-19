"""
ETL Script for Production Data

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
import time

# Record the start time
start_time = time.time()


# load .env file
load_dotenv()

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
days = 2
kingTannerUsername = str(os.getenv('SQL_SERVER_MICHAEL_TANNER_USERNAME'))
kingTannerPassword = str(os.getenv('SQL_SERVER_MICHAEL_TANNER_PASSWORD'))

### BEGIN PRODUCTION ETL PROCESS ###

print("Starting etl_joyn")

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
    tableName="header_data",
    uid=kingTannerUsername,
    password=kingTannerPassword
)

# Get Historical Daily Well Reading from SQL Server

historicalDailyWellReading = tech.getData(
    server=kingLiveServer,
    database=kingProductionDatabase,
    tableName="daily_well_reading",
    uid=kingTannerUsername,
    password=kingTannerPassword
        
)

print("Length of Historical Daily Well Reading: " + str(len(historicalDailyWellReading)))

# Get last 2 days of daily well reading from JOYN

joynDailyWellReading = joyn.getDailyWellReading(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    daysToLookBack=days
)

# Compare the two dataframes and get the duplicates
duplicatedIdList = joyn.compareJoynSqlDuplicates(
    joynData=joynDailyWellReading,
    sqlData=historicalDailyWellReading
)

listOfIds = duplicatedIdList['UUID'].tolist()

# Delete duplicate from Historical daily well reading

print("Length of Duplicate List: " + str(len(listOfIds)))

deleteRecords = tech.deleteDuplicateRecords(
    server=kingLiveServer,
    database=kingProductionDatabase,
    tableName="daily_well_reading",
    duplicateList=listOfIds
)

# Get New Length of Historical Daily Well Reading

historicalDailyWellReading = tech.getData(
    server=kingLiveServer,
    database=kingProductionDatabase,
    tableName="daily_well_reading",
    uid=kingTannerUsername,
    password=kingTannerPassword
        
)

print("Length of Historical Daily Well Reading After Deleting: " + str(len(historicalDailyWellReading)))

# Append joyn daily well reading to historical daily well reading

tech.putDataAppend(
    server=kingLiveServer,
    database=kingProductionDatabase,
    data=joynDailyWellReading,
    tableName="daily_well_reading",
    uid=kingTannerUsername,
    password=kingTannerPassword
)

# Get New Length of Historical Daily Well Reading

historicalDailyWellReading = tech.getData(
    server=kingLiveServer,
    database=kingProductionDatabase,
    tableName="daily_well_reading",
    uid=kingTannerUsername,
    password=kingTannerPassword
)

print("Length of Historical Daily Well Reading After Appending: " + str(len(historicalDailyWellReading)))


# Gets Historical Allocated Production from KOC Datawarehouse 3.0
historicalAllocatedProduction = tech.getData(
    server=kingLiveServer,
    database= kingProductionDatabase,
    tableName= "daily_production",
    uid=kingTannerUsername,
    password=kingTannerPassword
)

print("Length of Historical Allocated Production: " + str(len(historicalAllocatedProduction)))

# Get Last _ Days of Production From JOYN
joynProduction = joyn.getDailyAllocatedProductionRawWithDeleted(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    daysToLookBack=days
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
    tableName="daily_production",
    uid=kingTannerUsername,
    password=kingTannerPassword
)

print("All Historical Record Length After Deleting: " + str(len(lengthOfWorkingTable)))

## Append joyn production to historical allocated production
tech.putDataAppend(
    server=kingLiveServer,
    database=kingProductionDatabase,
    data=joynProduction,
    tableName="daily_production",
    uid=kingTannerUsername,
    password=kingTannerPassword
)

historicalAllocatedProduction = tech.getData(
    server=kingLiveServer,
    database=kingProductionDatabase,
    tableName="daily_production",
    uid=kingTannerUsername,
    password=kingTannerPassword
)

print("All Historical Record Length After Appending: " + str(len(historicalAllocatedProduction)))

# Record the end time
end_time = time.time()

# Calculate the elapsed time in seconds
elapsed_time_seconds = end_time - start_time

# Convert seconds to minutes with two decimal places
elapsed_time_minutes = elapsed_time_seconds / 60

dateEnd = dt.datetime.today()

runtimeSeconds = (dateEnd - dateToday).total_seconds()

usageFunction = king.updateUsageStatsEtlRuntime(
    etlStartTime=dateToday,
    etlEndTime=dateEnd,
    function=function,
    runtime=runtimeSeconds
)

print("Finished etl_joyn")
