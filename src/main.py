""" 
Main Script for KOC v2 Python Packages

Developed by: Michael Tanner

"""
# KOC v2 Python Packages
from kingscripts.production import greasebook, combocurve
from kingscripts.random import enverus

# Python Packages
from dotenv import load_dotenv
from enverus_developer_api import DeveloperAPIv3
import os

# load .env file
load_dotenv()

# getting API keys
apiKey = DeveloperAPIv3(secret_key=os.getenv('ENVERUS_API'))
greasebookApiKey = str(os.getenv('GREASEBOOK_API_KEY'))

# Enverus Stack
enverus.getWellData(apiKey, "42033325890000")
enverus.checkWellStatus(
    akiKey=apiKey, operatorName="BROWNING OIL", basin="MIDLAND")

# Greasebook Stack
greasebook.getProductionData(
    pullProd=False, days=30, greasebookApi=greasebookApiKey)


print("Main Script Complete")
