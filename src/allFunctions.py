"""
All Script for KOC Python Packages

Developer: Michael Tanner

"""
# KOC v3.0.2 Python Packages
from kingscripts.operations import greasebook, combocurve, joyn
from kingscripts.analytics import enverus, king
from kingscripts.afe import afe
from kingscripts.finance import tech, wenergy

# Python Packages
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
# Working Directories
workingDirectoryData = os.getenv("WORKING_DIRECTORY_DATA")
kocDatawarehouse = os.getenv("KOC_DATAWAREHOUSE")

# getting API keys
enverusApiKey = os.getenv('ENVERUS_API')
greasebookApi = os.getenv('GREASEBOOK_API_KEY')
serviceAccount = ServiceAccount.from_file(
    os.getenv("COMBOCURVE_API_SEC_CODE_LIVE"))
comboCurveApiKey = os.getenv("COMBOCURVE_API_KEY_PASS_LIVE")
kingServer = str(os.getenv('SQL_SERVER_KING_DATAWAREHOUSE'))
kingDatabase = str(os.getenv('SQL_KING_DATABASE'))
joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))

# variables
itSqlTable = "itSpend"


"""

ALL FUNCTIONS

"""
# GET DATA FROM king-arc1 server
data = tech.getData(
    serverName=kingServer,
    databaseName=kingDatabase,
    tableName=itSqlTable
)

tech.putData(
    server=kingServer,
    database=kingDatabase,
)

# AFE Stack Miller Ranch A502V
afe.dailyCost(
    workingDataDirectory=kocDatawarehouse,
    name="INSERT NAME HERE"
)
afe.variance(
    workingDataDirectory=kocDatawarehouse,
    name="INSERT NAME HERE"
)

# Combine AFE files and place in data warehouse
afe.combineAfeFiles(
    workingDataDirectory=kocDatawarehouse,
    listOfWells="INSERT NAME HERE"
)

# Gets Browning 518H Production Data
browing518HProductionMonthtlyData = enverus.getWellProductionData(
    apiKey=enverusApiKey,
    wellApi14="INSERT WELL API14 HERE"
)

enverus.checkWellStatus(
    apiKey=enverusApiKey,
    operatorName="INSERT OPERATOR NAME HERE",
    basin="INSERT BASIN HERE",
)

king.sendEmail(
    emailRecipient="INSERT EMAIL RECIPIENT HERE",
    emailRecipientName="INSERT EMAIL RECIPIENT NAME HERE",
    emailSubject="INSERT EMAIL SUBJECT HERE",
    emailMessage="INSERT EMAIL MESSAGE HERE",
    nameOfFile="INSERT NAME OF FILE HERE", # can define as None if no attachment
    attachment="INSERT ATTACHMENT HERE" # can define as None if no attachment
)

king.getAverageDailyVolumes(
    masterKingProdData="INSERT MASTER ALLOCATED PRODUCTION DATA HERE",
    startDate="INSERT START DATE HERE",
    endDate="INSERT END DATE HERE",
)

king.getNotReportedPumperList(
    masterKingProdData="INSERT MASTER ALLOCATED PRODUCTION DATA HERE",
    checkDate="INSERT CHECK DATE HERE",
)

king.createPumperMessage(
    badPumperData="INSERT BAD PUMPER DATA HERE",
    badPumperTrimmedList="INSERT BAD PUMPER TRIMMED LIST HERE",
    badPumperMessage="INSERT BAD PUMPER MESSAGE HERE",
)

combocurve.putJoynWellProductionData(
    currentJoynData="INSERT CURRENT JOYN DATA HERE",
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey,
)

combocurve.putGreasebookWellProductionData(
    workingDataDirectory=kocDatawarehouse,
    pullFromAllocation=False,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey,
    greasebookApi=greasebookApi,
    daysToPull=30
)

combocurve.getLatestScenarioOneLiner(
    workingDataDirectory=kocDatawarehouse,
    projectIdKey="INSERT PROJECT ID KEY HERE",
    scenarioIdKey="INSERT SCENARIO ID KEY HERE",
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

combocurve.getDailyForecastVolume(
    projectIdKey="INSERT PROJECT ID KEY HERE",
    forecastIdKey="INSERT FORECAST ID KEY HERE",
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

combocurve.getLatestScenarioMonthly(
    projectIdKey="INSERT PROJECT ID KEY HERE",
    scenarioIdKey="INSERT SCENARIO ID KEY HERE",
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

joyn.getDailyAllocatedProduction(
    workingDataDirectory=kocDatawarehouse,
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    daysToLookBack=2
)

joyn.mergeBIntoA(
    A="INSERT DATAFRAME HERE",
    B="INSERT DATAFRAME HERE",
)

print("done")
