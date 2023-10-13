"""
Main Script for KOC Python Packages 3.3.0

Developer: Michael Tanner

"""
# KOC v3.3.0 Python Packages
from kingscripts.operations import greasebook, combocurve, joyn
from kingscripts.analytics import enverus, king, tech
from kingscripts.afe import afe
from kingscripts.finance import wenergy

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
# Working Directories
workingDirectoryData = os.getenv("WORKING_DIRECTORY_DATA")
kocDatawarehouse = os.getenv("KOC_DATAWAREHOUSE")

# getting API keys
enverusApiKey = os.getenv('ENVERUS_API')
greasebookApi = os.getenv('GREASEBOOK_API_KEY')
serviceAccount = ServiceAccount.from_file(
    os.getenv("COMBOCURVE_API_SEC_CODE_LIVE"))
comboCurveApiKey = os.getenv("COMBOCURVE_API_KEY_PASS_LIVE")
joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))

## SQL Server Variables - for KOC Datawarehouse 3.0
kingServerExpress = str(os.getenv('SQL_SERVER'))
kingDatabaseExpress = str(os.getenv('SQL_KING_DATABASE'))
kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATAWAREHOUSE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))
kingPlanningDatabase = str(os.getenv('SQL_PLANNING_DATABASE'))
badPumperTable = "prod_bad_pumper_data"
kingPlanningDataRaw = str(os.getenv("KING_PLANNING_DATA_RAW"))
kingPlanningData = pd.read_excel(kingPlanningDataRaw)

## Names of KOC Employees
michaelTanner = os.getenv("MICHAEL_TANNER_EMAIL")
michaelTannerName = os.getenv("MICHAEL_TANNER_NAME")
gabeTatman = os.getenv("GABE_TATMAN_EMAIL")
gabeTatmanName = os.getenv("GABE_TATMAN_NAME")
garrettStacey = os.getenv("GARRETT_STACEY_EMAIL")
garrettStaceyName = os.getenv("GARRETT_STACEY_NAME")
nathanMyers = os.getenv("NATHAN_MYERS_EMAIL")
nathanMyersName = os.getenv("NATHAN_MYERS_NAME")

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

'''
WORKING ZONE
'''

king.getReadHowardProduction()

x = 5


# # AFE Stack Miller Ranch A501MH
# afe.dailyCost(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerrancha501mh
# )
# afe.variance(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerrancha501mh
# )

# # AFE Stack Miller Ranch B501MH
# afe.dailyCost(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerranchb501mh
# )
# afe.variance(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerranchb501mh
# )

# # AFE Stack Miller Ranch C301
# afe.dailyCost(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerranchc301
# )
# afe.variance(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerranchc301
# )

# # AFE Stack Miller Ranch C302MH
# afe.dailyCost(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerranchc302mh
# )
# afe.variance(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerranchc302mh
# )

# # AFE Stack Miller Ranch C302MH
# afe.dailyCost(
#     workingDataDirectory=kocDatawarehouse,
#     name=wu105
# )
# afe.variance(
#     workingDataDirectory=kocDatawarehouse,
#     name=wu105
# )

# # AFE Stack Miller Ranch C302MH
# afe.dailyCost(
#     workingDataDirectory=kocDatawarehouse,
#     name=wu108
# )
# afe.variance(
#     workingDataDirectory=kocDatawarehouse,
#     name=wu108
# )

# # AFE Stack Miller Ranch C302MH
# afe.dailyCost(
#     workingDataDirectory=kocDatawarehouse,
#     name=kinga199cv1h
# )
# afe.variance(
#     workingDataDirectory=kocDatawarehouse,
#     name=kinga199cv1h
# )

# # AFE Stack Miller Ranch C302MH
# afe.dailyCost(
#     workingDataDirectory=kocDatawarehouse,
#     name=kinga199cv2h
# )
# afe.variance(
#     workingDataDirectory=kocDatawarehouse,
#     name=kinga199cv2h
# )


# # Combine AFE files and place in data warehouse
# afe.combineAfeFiles(
#     workingDataDirectory=kocDatawarehouse,
#     listOfWells=listOfWells
# )

# joynUsers = joyn.getJoynUsers(
#     joynUsername=joynUsername,
#     joynPassword=joynPassword,
#     nameToFind="mtanner"
# )


# data = combocurve.getLatestScenarioMonthly(
#     projectIdKey="612fc3d36880c20013a885df",
#     scenarioIdKey="64aca0abaa25aa001201b299",
#     serviceAccount=serviceAccount,
#     comboCurveApi=comboCurveApiKey
# )

# crest = combocurve.ccScenarioToCrestFpSingleWell(
#     comboCurveScenarioData=data,
#     nglYield=1,
#     gasBtuFactor=1,
#     gasShrinkFactor=0,
#     oilPricePercent=.98,
#     gasPricePercent=1,
#     nglPricePercent=.3,
#     oilVariableCost=0,
#     gasVariableCost=0,
#     nglVariableCost=0,
#     waterVariableCost=0,
#     state="texas"
# )

# crestPdp = combocurve.ccScenarioToCrestFpPdp(
#     comboCurveScenarioData=data,
#     nglYield=1,
#     gasBtuFactor=1,
#     gasShrinkFactor=0,
#     oilPricePercent=1,
#     gasPricePercent=1,
#     nglPricePercent=.3,
# )


# crestPdp.to_excel(r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\production\crestPdpMillerRanchB501mhP90.xlsx", index=False)

# # AFE Stack Miller Ranch B501MH
# afe.dailyCost(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerrancha501mh
# )
# afe.variance(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerrancha501mh
# )

# data = pd.read_excel(r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\production\masterAllocatedProductionData.xlsx")

# #convert date to datetime
# data["Date"] = pd.to_datetime(data["Date"])

# data.to_excel(r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\production\masterAllocatedProductionDataDateTime.xlsx", index=False)

# joyn.getJoynUsers(
#     joynUsername=joynUsername,
#     joynPassword=joynPassword
# )

# Gets Browning 518H Production Data
browing518HProductionMonthtlyData = enverus.getWellProductionData(
    apiKey=enverusApiKey,
    wellApi14=browning5181H
)

# # Allocate Wells From Greasebook
# allocatedProductionData = greasebook.allocateWells(
#     days=50,
#     workingDataDirectory=kocDatawarehouse,
#     greasebookApi=greasebookApi,
#     pullProd=False,
#     edgeCaseRollingAverage=7
# )

# # KOC Datawarehouse LIVE DUMP
# print("Begin Exporting Greasebook Allocated Production Data to KOC Datawarehouse...")
# allocatedProductionData.to_excel(
#     kocDatawarehouse + r"\production\comboCurveAllocatedProductionJoynUpload09102023.xlsx", index=False)
# allocatedProductionData.to_json(
#     kocDatawarehouse + r"\production\comboCurveAllocatedProductionJoynUpload09102023.json", orient="records")
# print("Finished Exporting Greasebook Allocated Production Data to KOC Datawarehouse!")

# JOYN STACK
# DAILY ALLOCATED PRODUCTION
joynData = joyn.getDailyAllocatedProduction(
    workingDataDirectory=kocDatawarehouse,
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    daysToLookBack=2
)

print("Begin Exporting Master Joyn Data to KOC Datawarehouse...")
joynData.to_excel(kocDatawarehouse +
                  r"\production\masterJoynData.xlsx", index=False)
print("Finished Exporting Master Joyn Data to KOC Datawarehouse!")


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

## Update King Planning Ghantt Chart into SQL Server - use putData
king.updateKingPlanningChart(
    dataplan=kingPlanningData,
    serverName=kingLiveServer,
    databaseName=kingPlanningDatabase,
    tableName="prod_king_planning_data"
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

print("Finished Running KOC Daily Scripts! You rock man")
