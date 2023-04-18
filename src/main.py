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
workingDirectory = os.getenv("WORKING_DIRECTORY")

# getting API keys
enverusApi = DeveloperAPIv3(secret_key=os.getenv('ENVERUS_API'))
greasebookApiKey = os.getenv('GREASEBOOK_API_KEY')
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
MAIN SCRIPTS - see mainEnverus.py, mainGreasebook.py, mainComboCurve.py, and mainAFE.py for more details

'''

# Enverus Stack
browingWell = enverus.getWellData(
    apiKey=enverusApi,
    wellApi=browning518H
)

enverus.checkWellStatus(
    apiKey=enverusApi,
    operatorName=browningOperatorName,
    basin=basin
)

# Greasebook Stack
greasebook.getBatteryProductionData(
    workingDirectory=workingDirectory,
    pullProd=False,
    days=30,
    greasebookApi=greasebookApiKey
)

# ComboCurve Stack
combocurve.putWellProductionData(
    workingDirectory=workingDirectory,
    pullFromAllocation=False,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey,
    greasebookApi=greasebookApiKey,
    daysToPull=60
)

combocurve.getLatestScenario(
    workingDirectory=workingDirectory,
    projectIdKey=comboCurveProjectId,
    scenarioIdKey=comboCurveScenarioId,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

# AFE Stack
afe.dailyCost(
    workingDirectory=workingDirectory,
    name=afeWellName
)
afe.variance(
    workingDirectory=workingDirectory,
    name=afeWellName
)

# WELL COMMENTS

greasebookComments = greasebook.getComments(
    workingDirectory=workingDirectory,
    apiKey=greasebookApiKey
)

combocurve.putWellComments(
    cleanJson=greasebookComments,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

print("Main Script Complete")
