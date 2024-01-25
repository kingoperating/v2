"""
Main ETL for the JOYN - ComboCurve ETL Process

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

## api keys
comboCurveApiKey = os.getenv("COMBOCURVE_API_KEY_PASS_LIVE")
joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))

function = "etl_combocurve"

## Server Info
kingDatabaseExpress = str(os.getenv('SQL_KING_DATABASE'))
kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATAWAREHOUSE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))
serviceAccount = ServiceAccount.from_file(
    os.getenv("COMBOCURVE_API_SEC_CODE_LIVE"))


## BEGIN ##

print("Starting etl_combocurve")

# Get Production Data From SQL
production = tech.getData(
    server=kingLiveServer,
    database=kingProductionDatabase,
    tableName="daily_production",
)

headerData = tech.getData(
    server=kingLiveServer,
    database="wells",
    tableName="header_data",
)

# PUT production data in SQL Server
combocurve.putJoynWellProductionData(
    allocatedProductionMaster=production,
    comboCurveApi=comboCurveApiKey,
    serviceAccount=serviceAccount,
    daysToLookback=5,
    headerData=headerData,
)

print("Finished etl_combocurve")
