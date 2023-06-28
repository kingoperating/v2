
# Author: Gabe Tatman
# Date: 06/22/23
##

from kingscripts.operations import greasebook
from kingscripts.analytics import enverus

# Python Packages
from dotenv import load_dotenv
from combocurve_api_v1 import ServiceAccount
import os
import datetime as dt
from datetime import timedelta

# load .env file
load_dotenv()

# getting API Key
enverusApiKey = os.getenv('ENVERUS_API')
greasebookApi = os.getenv('GREASEBOOK_API_KEY')

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

totalAssetProduction = NaughtyList[1]
totalAssetProduction.to_csv(
    r"C:\Users\gtatman\OneDrive - King Operating\KOC Datawarehouse\totalAssetProduction.csv", index=False)

# greasebookComments = greasebook.getComments(
#     workingDataDirectory=kocDatawarehouse,
#     greasebookApi=greasebookApi
# )

allocatedProductionData = greasebook.allocateWells(
    days=30,
    workingDataDirectory=kocDatawarehouse,
    greasebookApi=greasebookApi,
    pullProd=False,
    edgeCaseRollingAverage=7
)

# # Backup
# allocatedProductionData.to_json(
#     workingDirectoryData + r"\comboCurveAllocatedProduction.json", orient="records")
# allocatedProductionData.to_csv(
#     workingDirectoryData + r"\comboCurveAllocatedProduction.csv", index=False)

# KOC Datawarehouse LIVE DUMP
allocatedProductionData.to_csv(
    kocDatawarehouse + r"\production\comboCurveAllocatedProduction.csv", index=False)
allocatedProductionData.to_json(
    kocDatawarehouse + r"\production\comboCurveAllocatedProduction.json", orient="records")
allocatedProductionData.to_csv(
    kocDatawarehouse + r"\production\allocationVersionControl\comboCurveAllocatedProduction_" + yesDateString + ".csv", index=False)


x = 5
