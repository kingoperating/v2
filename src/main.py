"""
Main Script for KOC v2 Python Packages

Developed by: Michael Tanner

"""
# KOC v2 Python Packages
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

# AFE Stack
afe.dailyCost(name="millerranchb501mh")
afe.variance(name="millerranchb501mh")

# Enverus Stack
enverus.getWellData(
    apiKey=enverusApi,
    wellApi="42033325890000"
)

enverus.checkWellStatus(
    apiKey=enverusApi,
    operatorName="BROWNING OIL",
    basin="MIDLAND"
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

combocurve.getLatestScenario(
    projectIdKey="612fc3d36880c20013a885df",
    scenarioIdKey="632e70eefcea66001337cd43",
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)


print("Main Script Complete")
