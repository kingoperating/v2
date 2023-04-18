"""
Main Script for KOC v2 Python Packages

Developed by: Michael Tanner

"""
# KOC v2.0.2 Python Packages
from kingscripts.operations import greasebook, combocurve
from kingscripts.analytics import enverus
from kingscripts.afe import afe

# Python Packages
from dotenv import load_dotenv
from enverus_developer_api import DeveloperAPIv3
from combocurve_api_v1 import ServiceAccount
import os

# load .env file
load_dotenv()

# Working Directory
workingDirectoryData = os.getenv("WORKING_DIRECTORY_DATA")

# getting API keys
enverusApi = DeveloperAPIv3(secret_key=os.getenv('ENVERUS_API'))
greasebookApi = os.getenv('GREASEBOOK_API_KEY')
serviceAccount = ServiceAccount.from_file(os.getenv("API_SEC_CODE_LIVE"))
comboCurveApiKey = os.getenv("API_KEY_PASS_LIVE")

# Important Variables for scripts
browning518H = "17047210530000"
browningOperatorName = "BROWNING OIL"
basin = "MIDLAND"
comboCurveProjectId = "612fc3d36880c20013a885df"
comboCurveScenarioId = "632e70eefcea66001337cd43"
afeWellName = "millerranchb501mh"

'''
WORKING ZONE

'''

'''
MAIN SCRIPTS - see mainEnverus.py, mainGreasebook.py, mainComboCurve.py, and mainAFE.py for more details

'''

# Enverus Stack
browingWell = enverus.getWellData(
    apiKey=enverusApi,
    wellApi14=browning518H
)

enverus.checkWellStatus(
    apiKey=enverusApi,
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
    name=afeWellName
)
afe.variance(
    workingDataDirectory=workingDirectoryData,
    name=afeWellName
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
