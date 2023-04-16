""" 
Pulling specific data from Enervus API

Developed by: Michael Tanner 

"""

from enverus_developer_api import DeveloperAPIv3
import requests
import os
import pandas as pd
from datetime import date, datetime, timedelta
import datetime as dt
import re


def getWellData(apiKey, wellApi):
    # Checks mos recent data from Browning Oil FLuvanna Unit
    for row in apiKey.query("production", API_UWI_14_UNFORMATTED=wellApi):
        totalProdMonths = row['ProducingDays']
        totalOil = row['Prod_OilBBL']
        updateDate = row['UpdatedDate']
        if "T" in updateDate:
            index = updateDate.index("T")
            updateDateBetter = updateDate[0:index]
        print("Date Of Browning Well Update: " + updateDateBetter)
        print("Daily Oil Rate: " + str(totalOil/totalProdMonths))


def checkWellStatus(apiKey, operatorName, basin):

    # gets relvent date values
    dateToday = dt.datetime.today()
    todayYear = dateToday.strftime("%Y")
    todayMonth = dateToday.strftime("%m")
    todayDay = dateToday.strftime("%d")

    todayYear = int(dateToday.strftime("%Y"))
    todayMonth = int(dateToday.strftime("%m"))
    todayDay = int(dateToday.strftime("%d"))

    dateList = []
    browningDate = []
    apiList = []

    for row in apiKey.query("detected-well-pads", ENVOperator=operatorName, ENVBasin=basin):
        updateDateWells = row['UpdatedDate']
    if "T" in updateDateWells:
        index = updateDateWells.index("T")
        updateDateWells = updateDateWells[0:index]
        dateList.append(updateDateWells)

    for i in range(0, len(dateList)):
        stringDate = dateList[i]
        splitDate = re.split("-", stringDate)
        day = int(splitDate[2])  # gets the correct day
        month = int(splitDate[1])  # gets the correct month
        year = int(splitDate[0])  # gets the correct

        if year == todayYear and month == todayMonth and day == todayDay:
            for row in apiKey.query("wells", ENVOperator="BROWNING OIL", ENVBasin="MIDLAND"):
                apiNumber = row['API_UWI_14_Unformatted']
                browningDates = row['UpdatedDate']
                apiList.append(apiNumber)
                browningDate.append(browningDates)
            print("SOMETHING HAS BEEN UPDATED.....")

    print("Completed Looking for Updates on Browning Oil FLuvanna Unit")
