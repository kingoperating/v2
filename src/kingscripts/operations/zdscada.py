### High Level Code for zdScada
### Developed by Gabe Tatman and Michael Tanner

# Importing Python Packages
from dotenv import load_dotenv
import os
import datetime as dt
from datetime import timedelta
import pandas as pd
import requests
import json

# Load .env file
load_dotenv()



# https://zdscada.com/zdapi/auth/login?password=dPT6SGHyp8BP&username=KingOperating_API&companyId=876


def getScadaToken(username, password, companyid):
    
    url = "https://zdscada.com/zdapi/auth/login?password=" + password + "&username=" + username + "&companyId=" + companyid
    # make post request
    response = requests.post(url)
    # get token from response
    token = response.json()['token']

    x = 5