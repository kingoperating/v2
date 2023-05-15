"""
Main Script for KOC v2 Python Packages

Developed by: Michael Tanner

"""
# KOC v2.0.6 Python Packages
from kingscripts.operations import greasebook, combocurve
from kingscripts.analytics import enverus
from kingscripts.afe import afe

# Python Packages
from dotenv import load_dotenv
from combocurve_api_v1 import ServiceAccount
import os

# load .env file
load_dotenv()

'''
FIRST - MAKE SURE ALL THE ENVIRONMENT VARIABLES ARE SET IN THE .ENV FILE 

SECOND - ENSURE YOUR WORKING DATA DIRECTORY IS SET TO THE CORRECT FOLDER. 

'''

# Working Directory
workingDirectoryData = os.getenv("WORKING_DIRECTORY_DATA")
kocDatawarehouse = os.getenv("KOC_DATAWAREHOUSE")

# getting API keys
enverusApiKey = os.getenv('ENVERUS_API')
greasebookApi = os.getenv('GREASEBOOK_API_KEY')
serviceAccount = ServiceAccount.from_file(
    os.getenv("COMBOCURVE_API_SEC_CODE_LIVE"))
comboCurveApiKey = os.getenv("COMBOCURVE_API_KEY_PASS_LIVE")

# Important Variables for scripts
browning5181H = "42033325890000"
browningOperatorName = "BROWNING OIL"
basin = "MIDLAND"
comboCurveProjectId = "612fc3d36880c20013a885df"
comboCurveScenarioId = "632e70eefcea66001337cd43"
millerranchb501mh = "millerranchb501mh"
millerrancha501mh = "millerrancha501mh"
nameOfWell = "millerranchb501mh"


'''
WORKING ZONE

'''

browing518HProductionMonthtlyData = enverus.getWellProductionData(
    apiKey=enverusApiKey,
    wellApi14=browning5181H
)

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
    days=30,
    workingDataDirectory=kocDatawarehouse,
    greasebookApi=greasebookApi,
    pullProd=False
)

allocatedProductionData.to_json(
    workingDirectoryData + r"\comboCurveAllocatedProduction.json", orient="records")
allocatedProductionData.to_csv(
    workingDirectoryData + r"\comboCurveAllocatedProduction.csv", index=False)
allocatedProductionData.to_csv(
    r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\comboCurveAllocatedProduction.csv", index=False)

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
    workingDataDirectory=workingDirectoryData,
    name=millerranchb501mh
)
afe.variance(
    workingDataDirectory=workingDirectoryData,
    name=millerranchb501mh
)

# AFE Stack Miller Ranch A501MH
afe.dailyCost(
    workingDataDirectory=workingDirectoryData,
    name=millerrancha501mh
)
afe.variance(
    workingDataDirectory=workingDirectoryData,
    name=millerrancha501mh
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

pdpModel = combocurve.getLatestScenario(
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
