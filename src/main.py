"""
Main Script for KOC Python Packages

Developer: Michael Tanner

"""
# KOC v3.0.0 Python Packages
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
kingServer = str(os.getenv('SQL_SERVER'))
kingDatabase = str(os.getenv('SQL_KING_DATABASE'))
joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))
michaelTanner = os.getenv("MICHAEL_TANNER_EMAIL")
michaelTannerName = os.getenv("MICHAEL_TANNER_NAME")
gabeTatman = os.getenv("GABE_TATMAN_EMAIL")
gabeTatmanName = os.getenv("GABE_TATMAN_NAME")

# Getting Date Variables
dateToday = dt.datetime.today()
dateYesterday = dateToday - timedelta(days=1)
todayYear = dateToday.strftime("%Y")
todayMonth = dateToday.strftime("%m")
todayDay = dateToday.strftime("%d")

yesYear = int(dateYesterday.strftime("%Y"))
yesMonth = int(dateYesterday.strftime("%m"))
yesDay = int(dateYesterday.strftime("%d"))
yesDateString = dateYesterday.strftime("%Y-%m-%d")
todayDateString = dateToday.strftime("%Y-%m-%d")

# Important Variables for scripts
browning5181H = "42033325890000"
browningOperatorName = "BROWNING OIL"
basin = "MIDLAND"
comboCurveProjectId = "612fc3d36880c20013a885df"
comboCurveScenarioId = "632e70eefcea66001337cd43"
comboCurveForecastId = "6495f222b89c6d001250fbf9"
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
irvinsisters53m1h = "irvinsisters53m1h"
pshigoda752h = "pshigoda752h"
itSqlTable = "itSpend"
daysToPull = 35
daysToLookBack = 7
testFile = r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\assetPrettyName.xlsx"
listOfWells = [
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

'''
WORKING ZONE

'''
# IT SPEND
itSpend = tech.getItSpend(
    serverName=kingServer,
    databaseName=kingDatabase,
    tableName=itSqlTable
)

# Gets Browning 518H Production Data
browing518HProductionMonthtlyData = enverus.getWellProductionData(
    apiKey=enverusApiKey,
    wellApi14=browning5181H
)

# exports results for number of records in the dataframe
browing518HProductionMonthtlyData.to_excel(
    kocDatawarehouse + r"\browningWell.xlsx", index=False)

# Gresebook Stack
pumperNotReportedList = greasebook.getBatteryProductionData(
    workingDataDirectory=kocDatawarehouse,
    fullProd=False,
    days=daysToPull,
    greasebookApi=greasebookApi
)

# Allocate Wells From Greasebook
allocatedProductionData = greasebook.allocateWells(
    days=daysToPull,
    workingDataDirectory=kocDatawarehouse,
    greasebookApi=greasebookApi,
    pullProd=False,
    edgeCaseRollingAverage=7
)

# KOC Datawarehouse LIVE DUMP
print("Begin Exporting Greasebook Allocated Production Data to KOC Datawarehouse...")
allocatedProductionData.to_excel(
    kocDatawarehouse + r"\production\comboCurveAllocatedProduction.xlsx", index=False)
allocatedProductionData.to_json(
    kocDatawarehouse + r"\production\comboCurveAllocatedProduction.json", orient="records")
print("Finished Exporting Greasebook Allocated Production Data to KOC Datawarehouse!")

# JOYN STACK
# DAILY ALLOCATED PRODUCTION
joynData = joyn.getDailyAllocatedProduction(
    workingDataDirectory=kocDatawarehouse,
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    daysToLookBack=daysToLookBack
)

print("Begin Exporting Master Joyn Data to KOC Datawarehouse...")
joynData.to_excel(kocDatawarehouse +
                  r"\production\masterJoynData.xlsx", index=False)
print("Finished Exporting Master Joyn Data to KOC Datawarehouse!")

# Master Greasebook Data
print("Reading Master Greasebook Data...")
masterGreasebookData = pd.read_excel(os.getenv("MASTER_GREASEBOOK_DATA"))
print("Finished Reading Master Greasebook Data!")

# MERGE JOYN DATA WITH MASTER GREASEBOOK DATA
masterData = joyn.mergeBIntoA(
    A=masterGreasebookData,
    B=joynData
)

# EXPORT MASTER DATA TO KOC DATAWAREHOUSE
print("Begin Exporting Master Data to KOC Datawarehouse...")
masterData.to_excel(
    kocDatawarehouse + r"\production\masterAllocatedProductionData.xlsx", index=False)

masterData.to_json(kocDatawarehouse +
                   r"\production\masterAllocatedProductionData.json", orient="records")
print("Finished Exporting Master Data to KOC Datawarehouse!")

# ComboCurve PUT Statements
combocurve.putGreasebookWellProductionData(
    workingDataDirectory=kocDatawarehouse,
    pullFromAllocation=False,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey,
    greasebookApi=greasebookApi,
    daysToPull=daysToPull
)

combocurve.putJoynWellProductionData(
    currentJoynData=joynData,
    comboCurveApi=comboCurveApiKey,
    serviceAccount=serviceAccount
)

# AFE Stack Miller Ranch C301
afe.dailyCost(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchc301
)
afe.variance(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchc301
)

# AFE Stack Miller Ranch B501MH
afe.dailyCost(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchb501mh
)
afe.variance(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchb501mh
)

# AFE Stack Miller Ranch A501MH
afe.dailyCost(
    workingDataDirectory=kocDatawarehouse,
    name=millerrancha501mh
)
afe.variance(
    workingDataDirectory=kocDatawarehouse,
    name=millerrancha501mh
)

# AFE Stack Miller Ranch A502V
afe.dailyCost(
    workingDataDirectory=kocDatawarehouse,
    name=millerrancha502v
)
afe.variance(
    workingDataDirectory=kocDatawarehouse,
    name=millerrancha502v
)

# AFE Stack Miller Ranch A502V
afe.dailyCost(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchc302mh
)
afe.variance(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchc302mh
)

# Combine AFE files and place in data warehouse
afe.combineAfeFiles(
    workingDataDirectory=kocDatawarehouse,
    listOfWells=listOfWells
)

# King Module
subject = "KOC Daily Scripts - " + todayDateString
message = "KOC Daily Scripts successfully ran for " + yesDateString

message = message + "\n\n" + \
    "Refresh any PowerBi Dashboards that are connected to the KOC Datawarehouse."

# Send Email - attacment is optional
king.sendEmail(
    emailRecipient=michaelTanner,
    emailRecipientName=michaelTannerName,
    emailSubject=subject,
    emailMessage=message,
)

king.sendEmail(
    emailRecipient=gabeTatman,
    emailRecipientName=gabeTatmanName,
    emailSubject=subject,
    emailMessage=message,
)


'''
ALL SCRIPTS - see mainEnverus.py, mainGreasebook.py, mainComboCurve.py, and mainAFE.py for more details

'''

# Enverus Stack
browing518HProductionMonthtlyData = enverus.getWellData(
    apiKey=enverusApiKey,
    wellApi14=browning5181H
)

browing518HProductionMonthtlyData.to_excel(
    workingDirectoryData + r"\browningWell.xlsx", index=False)
updateDate = browing518HProductionMonthtlyData["Date"][1]
print("Last update date: " + updateDate)

enverus.checkWellStatus(
    apiKey=enverusApiKey,
    operatorName=browningOperatorName,
    basin=basin
)

# Greasebook Stack
greasebook.getBatteryProductionData(
    workingDataDirectory=workingDirectoryData,
    fullProd=False,
    days=30,
    greasebookApi=greasebookApi
)

greasebook.sendPumperEmail(
    pumperNotReportedList=pumperNotReportedList, workingDataDirectory=workingDirectoryData
)

greasebook.allocateWells(
    days=30,
    workingDataDirectory=workingDirectoryData,
    greasebookApi=greasebookApi,
    pullProd=False
)

# WELL COMMENTS

greasebookComments = greasebook.getComments(
    workingDataDirectory=kocDatawarehouse,
    greasebookApi=greasebookApi,
    prodStartDate="2022-01-01",
    prodEndDate="2022-03-31"
)

# ComboCurve Stack
combocurve.putGreasebookWellProductionData(
    workingDataDirectory=workingDirectoryData,
    pullFromAllocation=False,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey,
    greasebookApi=greasebookApi,
    daysToPull=60
)

pdpModel = combocurve.getLatestScenarioOneLiner(
    workingDataDirectory=workingDirectoryData,
    projectIdKey=comboCurveProjectId,
    scenarioIdKey=comboCurveScenarioId,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

pdpModel.to_excel(workingDirectoryData +
                  r"\eurData.xlsx", index=False)


latestPdpDailyForecast = combocurve.getDailyForecastVolume(
    projectIdKey=comboCurveProjectId,
    forecastIdKey=comboCurveForecastId,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

latestPdpDailyForecast.to_excel(
    kocDatawarehouse + r"\production\latestDailyForecast.xlsx", index=False)

combocurve.putGreasebookWellComments(
    cleanJson=greasebookComments,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

combocurve.getDailyForecastVolume(
    workingDataDirectory=kocDatawarehouse,
    projectIdKey=comboCurveProjectId,
    forecastIdKey=comboCurveForecastId,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

# AFE Stack
afe.dailyCost(
    workingDataDirectory=workingDirectoryData,
    name=millerranchb501mh
)
afe.variance(
    workingDataDirectory=workingDirectoryData,
    name=millerranchb501mh
)
# combine AFE files
afe.combineAfeFiles(
    workingDataDirectory=kocDatawarehouse,
    listOfWells=listOfWells
)

# JOYN STACK
joynData = joyn.getDailyAllocatedProduction()
joynData.to_csv(workingDirectoryData +
                r"\joynAllocatedProductionGoodTestMergred.csv", index=False)
