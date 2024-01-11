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
from kingscripts.operations import joyn
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# load .env file
load_dotenv()

kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATABASE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))


# How to query

# data = tech.getData(
#     serverName=kingLiveServer,
#     databaseName=kingProductionDatabase,
#     tableName="prod_bad_pumper_data"
# )

# How to write 

# king_gabe_table = str(os.getenv('SQL_GABE_DATABASE'))
# tech.putData(
#     server=kingLiveServer,
#     database = king_gabe_table,
#     data = data,
#     tableName= "test_table"
# )
# print("Hooray")

# ** WORKING SPACE **

## Get JOYN Username and password
joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))

## Get the well header data that we want to pull from JOYN from main.py

wellHeaderData = joyn.getWellHeaderData(
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

## Connect to the SQL server and pull the latest master production table

# master_sql_prod_data = tech.getData(
#     serverName=kingLiveServer,
#     databaseName= "gabe",
#     tableName="test_table"
# )

## Pull latest X Days of modified production data from JOYN

dataframe = joyn.getDailyAllocatedProductionRawWithDeleted(joynUsername, joynPassword, wellHeaderData,365)



# dataframe.to_csv(r"C:\Users\gtatman\Downloads\Python\testproduction.csv")
tech.putData(
    server=kingLiveServer,
    database = "gabe",
    data = dataframe,
    tableName= "test_table"
)


