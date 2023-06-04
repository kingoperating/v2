"""
Main Script for KOC v2 Python Packages

Developed by: Michael Tanner - hi heheh

"""
# KOC v2.0.8 Python Packages
from kingscripts.operations import greasebook, combocurve, joyn
from kingscripts.analytics import enverus
from kingscripts.afe import afe
from kingscripts.finance import tech, wenergy

# Python Packages
from dotenv import load_dotenv
from combocurve_api_v1 import ServiceAccount
import os
import datetime as dt
from datetime import timedelta

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

# Important Variables for scripts
browning5181H = "42033325890000"
browningOperatorName = "BROWNING OIL"
basin = "MIDLAND"
comboCurveProjectId = "612fc3d36880c20013a885df"
comboCurveScenarioId = "632e70eefcea66001337cd43"
millerranchb501mh = "millerranchb501mh"
millerrancha502v = "millerrancha502v"
millerrancha501mh = "millerrancha501mh"
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

'''
WORKING ZONE

'''

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

# prints results for number of records in the dataframe
print("Number of Records in Fluvanna 518H: " +
      str(len(browing518HProductionMonthtlyData)))
browing518HProductionMonthtlyData.to_excel(
    workingDirectoryData + r"\browningWell.xlsx", index=False)
browing518HProductionMonthtlyData.to_excel(
    kocDatawarehouse + r"\browningWell.xlsx", index=False)

newBrowningFluvannaActivity = enverus.checkWellStatus(
    apiKey=enverusApiKey,
    operatorName=browningOperatorName,
    basin=basin
)

pumperNotReportedList = greasebook.getBatteryProductionData(
    workingDataDirectory=kocDatawarehouse,
    pullProd=False,
    days=30,
    greasebookApi=greasebookApi
)

greasebook.sendPumperEmail(
    pumperNotReportedList=pumperNotReportedList[0],
    workingDataDirectory=kocDatawarehouse
)

totalAssetProduction = pumperNotReportedList[1]
totalAssetProduction.to_csv(
    r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\totalAssetProduction.csv", index=False)

allocatedProductionData = greasebook.allocateWells(
    days=35,
    workingDataDirectory=kocDatawarehouse,
    greasebookApi=greasebookApi,
    pullProd=False,
    edgeCaseRollingAverage=7
)

# Backup
allocatedProductionData.to_json(
    workingDirectoryData + r"\comboCurveAllocatedProduction.json", orient="records")
allocatedProductionData.to_csv(
    workingDirectoryData + r"\comboCurveAllocatedProduction.csv", index=False)

# KOC Datawarehouse LIVE DUMP
allocatedProductionData.to_csv(
    kocDatawarehouse + r"\production\comboCurveAllocatedProduction.csv", index=False)
allocatedProductionData.to_json(
    kocDatawarehouse + r"\production\comboCurveAllocatedProduction.json", orient="records")
allocatedProductionData.to_csv(
    kocDatawarehouse + r"\production\allocationVersionControl\comboCurveAllocatedProduction_" + yesDateString + ".csv", index=False)

combocurve.putWellProductionData(
    workingDataDirectory=kocDatawarehouse,
    pullFromAllocation=False,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey,
    greasebookApi=greasebookApi,
    daysToPull=25
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


'''
MAIN SCRIPTS - see mainEnverus.py, mainGreasebook.py, mainComboCurve.py, and mainAFE.py for more details

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
    pullProd=False,
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

# ComboCurve Stack
combocurve.putWellProductionData(
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

# AFE Stack
afe.dailyCost(
    workingDataDirectory=workingDirectoryData,
    name=millerranchb501mh
)
afe.variance(
    workingDataDirectory=workingDirectoryData,
    name=millerranchb501mh
)

# WELL COMMENTS

greasebookComments = greasebook.getComments(
    workingDataDirectory=workingDirectoryData,
    greasebookApi=greasebookApi
)

combocurve.putWellComments(
    cleanJson=greasebookComments,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)
