""" 
Pulling specific data from Enervus API

Developed by: Michael Tanner 

"""

from enverus_developer_api import DeveloperAPIv3
import datetime as dt
import re
import pandas as pd
import datetime

'''
Returns a dataframe for a specific API of the monthly production data

'''


def getWellData(apiKey, wellApi):
    # Checks if API is 14 digits long
    if len(wellApi) != 14:
        lengthOfApi = len(wellApi)
        print("API is not 14 digits long. It is " +
              str(lengthOfApi) + " digits long.")
        return
    # checks to ensure correct class for Enverus API
    if type(apiKey) != DeveloperAPIv3:
        print("API Key is not the correct class")
        return
    # creates a dataframe for the well production data
    wellData = pd.DataFrame(columns=["API", "Date", "Oil", "Gas", "Water"])

    for row in apiKey.query("production", API_UWI_14_UNFORMATTED=wellApi):
        totalProdMonths = row['ProducingDays']
        totalOil = row['Prod_OilBBL']
        totalGas = row['Prod_MCFE']
        totalWater = row['WaterProd_BBL']
        updateDate = row['ProducingMonth']

        # spliting date correctly
        splitDate = re.split("T", updateDate)
        splitDate2 = re.split("-", splitDate[0])
        year = int(splitDate2[0])
        month = int(splitDate2[1])
        day = int(splitDate2[2])
        dateString = str(month) + "/" + str(day) + "/" + str(year)
        wellRow = [wellApi, dateString, totalOil, totalGas, totalWater]
        wellData.loc[len(wellData)+1] = wellRow

    # Reserve Dataframe
    wellDataSorted = wellData.iloc[::-1]
    return wellDataSorted


'''
Returns a dataframe of the updated well status for a specific operator and basin

'''


def checkWellStatus(apiKey, operatorName, basin):
    # checks to ensure correct class for Enverus API
    if type(apiKey) != DeveloperAPIv3:
        print("API Key is not the correct class")
        return
    # checks to ensure operatorName is string
    if type(operatorName) != str:
        print("Operator Name is not a string")
        return
    # checks to ensure basin is string
    if type(basin) != str:
        print("Basin is not a string")
        return

    # gets relvent date values
    dateToday = dt.datetime.today()
    todayYear = dateToday.strftime("%Y")
    todayMonth = dateToday.strftime("%m")
    todayDay = dateToday.strftime("%d")

    todayYear = int(dateToday.strftime("%Y"))
    todayMonth = int(dateToday.strftime("%m"))
    todayDay = int(dateToday.strftime("%d"))

    dateList = []
    statusDateList = []
    apiList = []

    wellStatus = pd.DataFrame(columns=["API", "Date", "Status"])

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
                statusDate = row['UpdatedDate']
                apiList.append(apiNumber)
                statusDateList.append(statusDate)
                dateString = str(month) + "/" + str(day) + "/" + str(year)
                statusRow = [apiNumber, dateString, "New Well"]
                wellStatus.loc[len(wellStatus)+1] = statusRow

        print("SOMETHING HAS BEEN UPDATED")
    print("Completed Looking for Updates on " +
          str(operatorName) + " in " + str(basin) + " Basin")

    return wellStatus
