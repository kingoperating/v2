
# Author: Gabe Tatman
# Date: 06/22/23
##

from kingscripts.operations import greasebook, joyn, combocurve
from kingscripts.analytics import enverus

# Python Packages
from dotenv import load_dotenv
from combocurve_api_v1 import ServiceAccount
import os
import datetime as dt
from datetime import timedelta
import pandas as pd

# load .env file
load_dotenv()

# getting API Key
enverusApiKey = os.getenv('ENVERUS_API')
greasebookApi = os.getenv('GREASEBOOK_API_KEY')
joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))

# Working Directories
# workingDirectoryData = os.getenv("WORKING_DIRECTORY_DATA") ## Backup file
kocDatawarehouse = os.getenv("KOC_DATAWAREHOUSE")


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
millerranchc301 = "millerranchc301"
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

# Working Zone

# End Working Zone

# Gets Browning 518H Production Data
browing518HProductionMonthtlyData = enverus.getWellProductionData(
    apiKey=enverusApiKey,
    wellApi14=browning5181H
)

# Greasebook Stack
NaughtyList = greasebook.getBatteryProductionData(
    workingDataDirectory=kocDatawarehouse,
    fullProd=False,
    days=30,
    greasebookApi=greasebookApi
)

# totalAssetProduction = NaughtyList[1]
# totalAssetProduction.to_csv(
#     r"C:\Users\gtatman\OneDrive - King Operating\KOC Datawarehouse\totalAssetProduction.csv", index=False)

allocatedProductionData = greasebook.allocateWells(
    days=30,
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


print("Yay!")
x = 5
