## Gabe's Test Environment for pulling SQL Data from Server ##

# Performing  GIT PUSH

# git checkout -b NameOfBRanch
# PS C:\GT_code\v2> git add .  
# PS C:\GT_code\v2> git commit -m "put comments here"
# git push origin NameOfBRanch
# Branch gets approved by admin after comments added to github

# git checkout main
# git stash (only if unsaved changes on existing branch)
# git pull origin main (to refresh after branch merged)

import os
from kingscripts.analytics import tech
from kingscripts.operations import joyn, combocurve
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from combocurve_api_v1 import ServiceAccount
import time


start_time = time.time()


# load .env file
load_dotenv()

kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATAWAREHOUSE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))

serviceAccount = ServiceAccount.from_file(
    os.getenv("COMBOCURVE_API_SEC_CODE_LIVE"))
comboCurveApiKey = os.getenv("COMBOCURVE_API_KEY_PASS_LIVE")


joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))


getWellHeaderData = joyn.getWellHeaderData(
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

# # Post header data to server
# tech.putDataReplace(
#     server=kingLiveServer,
#     database = "wells",
#     data = getWellHeaderData,
#     tableName= "header_data"
# )

# dailyProduction = joyn.getDailyAllocatedProductionRaw(
#     joynUsername=joynUsername,
#     joynPassword=joynPassword,
#     wellHeaderData=getWellHeaderData,
#     daysToLookBack = 365
# )

# # Post daily production to server

# tech.putDataReplace(
#     server=kingLiveServer,
#     database = "production",
#     data = dailyProduction,
#     tableName= "daily_production_2"
# )

# print("Daily Production Posted to SQL Server")

# end_time = time.time()

# elapsed_time_seconds = end_time - start_time

# # Convert seconds to minutes with two decimal places
# elapsed_time_minutes = elapsed_time_seconds / 60

# # Print the runtime in minutes with two decimal places
# print(f"Script execution time: {elapsed_time_minutes:.2f} minutes")

# x = 5

# get Daily Well Reading
DailyWellReading = joyn.getDailyWellReading(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    wellHeaderData=getWellHeaderData,
    daysToLookBack = 365
)

# Post Daily Well Reading to SQL Server
tech.putDataReplace(
    server=kingLiveServer,
    database = "production",
    data = DailyWellReading,
    tableName= "daily_well_reading"
)








# forecast = combocurve.getDailyForecastVolume(
#     projectIdKey="6572267d2bcb2950186aca90",
#     forecastIdKey="65722aa4ea66fa10d79f2be6",
#     serviceAccount=serviceAccount,
#     comboCurveApi = comboCurveApiKey
# )

# print(len(forecast))

# print("Writing forecast to SQL Server...")

# tech.putDataReplace(
#      server=kingLiveServer,
#      database = "production",
#      data = forecast,
#      tableName= "daily_forecast"

# )

# x = 5
# # How to query

# # data = tech.getData(
# #     serverName=kingLiveServer,
# #     databaseName=kingProductionDatabase,
# #     tableName="prod_bad_pumper_data"
# # )

# # How to write 

# # king_gabe_table = str(os.getenv('SQL_GABE_DATABASE'))
# # tech.putData(
# #     server=kingLiveServer,
# #     database = king_gabe_table,
# #     data = data,
# #     tableName= "test_table"
# # )
# # print("Hooray")


## Get the well header data that we want to pull from JOYN from main.py

# wellHeaderData = joyn.getWellHeaderData(
#     joynUsername=joynUsername,
#     joynPassword=joynPassword
# )

## Connect to the SQL server and pull the latest master production table

# master_sql_prod_data = tech.getData(
#     serverName=kingLiveServer,
#     databaseName= "gabe",
#     tableName="test_table"
# )

## Pull latest X Days of modified production data from JOYN

# dataframe = joyn.getDailyAllocatedProductionRawWithDeleted(joynUsername, joynPassword, wellHeaderData,5)



# # dataframe.to_csv(r"C:\Users\gtatman\Downloads\Python\testproduction.csv")
# tech.putDataReplace(
#     server=kingLiveServer,
#     database = "gabe",
#     data = dataframe,
#     tableName= "working_table"
# )


end_time = time.time()

elapsed_time_seconds = end_time - start_time

# Convert seconds to minutes with two decimal places
elapsed_time_minutes = elapsed_time_seconds / 60

# Print the runtime in minutes with two decimal places
print(f"Script execution time: {elapsed_time_minutes:.2f} minutes")