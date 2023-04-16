"""
Main Script for KOC v2 Python Packages

Developed by: Michael Tanner

"""
# KOC v2 Python Packages
from kingscripts.production import greasebook, combocurve
from kingscripts.random import enverus
from kingscripts.afe import afe

# Python Packages
from dotenv import load_dotenv
from enverus_developer_api import DeveloperAPIv3
from combocurve_api_v1 import ServiceAccount, ComboCurveAuth
import os

# load .env file
load_dotenv()

# getting API keys
comboCurveApi = DeveloperAPIv3(secret_key=os.getenv('ENVERUS_API'))
greasebookApiKey = str(os.getenv('GREASEBOOK_API_KEY'))
serviceAccount = ServiceAccount.from_file(os.getenv("API_SEC_CODE_LIVE"))
comboCurveApiKey = os.getenv("API_KEY_PASS_LIVE")

# Enverus Stack
enverus.getWellData(
    api=comboCurveApi,
    wellApi="42033325890000"
)

enverus.checkWellStatus(
    akiKey=comboCurveApi,
    operatorName="BROWNING OIL",
    basin="MIDLAND"
)

# AFE Stack
afe.dailyCost(name="millerranchb501mh")
afe.variance(name="millerranchb501mh")

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

print("Main Script Complete")
