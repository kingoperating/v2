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

# getting API keys
enverusApi = DeveloperAPIv3(secret_key=os.getenv('ENVERUS_API'))
greasebookApiKey = os.getenv('GREASEBOOK_API_KEY')
serviceAccount = ServiceAccount.from_file(os.getenv("API_SEC_CODE_LIVE"))
comboCurveApiKey = os.getenv("API_KEY_PASS_LIVE")

# Important Variables for scripts
enverusWellApiNumber = "42033325890000"
browningOperatorName = "BROWNING OIL"
basin = "MIDLAND"
comboCurveProjectId = "612fc3d36880c20013a885df"
comboCurveScenarioId = "632e70eefcea66001337cd43"
afeWellName = "millerranchb501mh"

'''
MAIN SCRIPTS - see mainEnverus.py, mainGreasebook.py, mainComboCurve.py, and mainAFE.py for more details

'''

# Enverus Stack
enverus.getWellData(
    apiKey=enverusApi,
    wellApi=enverusWellApiNumber
)

enverus.checkWellStatus(
    apiKey=enverusApi,
    operatorName=browningOperatorName,
    basin=basin
)

# Greasebook Stack
greasebook.getProductionData(
    pullProd=False,
    days=30,
    greasebookApi=greasebookApiKey
)

# ComboCurve Stack
combocurve.putWellProductionData(
    pullFromAllocation=False,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey,
    greasebookApi=greasebookApiKey,
    daysToPull=60
)

''' combocurve.getLatestScenario(
    projectIdKey=comboCurveProjectId,
    scenarioIdKey=comboCurveScenarioId,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
) '''

''' # AFE Stack
afe.dailyCost(name=afeWellName)
afe.variance(name=afeWellName) '''


print("Main Script Complete")
