""" 
Main Script for KOC v2 Python Packages

Developed by: Michael Tanner

"""


from kingscripts.production import greasebook
from kingscripts.random import enverus
from dotenv import load_dotenv
from enverus_developer_api import DeveloperAPIv3
import os

# load .env file
load_dotenv()


apiKey = DeveloperAPIv3(secret_key=os.getenv('ENVERUS_API'))

enverus.getWellData(apiKey, "42033325890000")
enverus.checkWellStatus(
    akiKey=apiKey, operatorName="BROWNING OIL", basin="MIDLAND")

print("yay")
