import requests
import json
import datetime as dt
import re
import pandas as pd
import os
from dotenv import load_dotenv
import numpy as np
from kingscripts.operations.combocurve import *
from combocurve_api_v1 import ServiceAccount

"""
    
This script gets all the modifed daily allocated production over to the master JOYN dataframe in prep to merge    

"""


def getDailyAllocatedProduction(workingDataDirectory):

    load_dotenv()

    # Read in Master Data
    masterAllocationData = pd.read_excel(
        workingDataDirectory + r"\master\masterWellAllocation.xlsx")
    masterJoynData = pd.read_excel(
        workingDataDirectory + r"\production\masterJoynData.xlsx")
    forecastedProductionData = pd.read_csv(
        workingDataDirectory + r"\production\forecastWellsFinal.csv")

    # Get id, API and Well Accounting Name from masterAllocationData
    joynIdList = masterAllocationData["JOYN Id"].tolist()
    apiNumberList = masterAllocationData["API"].tolist()
    wellAccountingNameList = masterAllocationData["Name in Accounting"].tolist(
    )

    # Functions
    def getForecast(apiNumber, date):

        indexList = [index for index, value in enumerate(
            forecastedProductionData["API 14"].to_list()) if value == apiNumber]

        # Gets the forecasted prodcution for the current date
        forecastedDateList = [forecastedProductionData["Date"][index]
                              for index in indexList]

        # finds the index of the current date in the forecasted data and sets to the current forecasted volume
        if date in forecastedDateList:
            forecastedIndex = forecastedDateList.index(date)
            oilVolumeForecast = forecastedProductionData["Oil"][indexList[forecastedIndex]]
            gasVolumeForecast = forecastedProductionData["Gas"][indexList[forecastedIndex]]
            waterVolumeForecast = forecastedProductionData["Water"][indexList[forecastedIndex]]
        else:
            oilVolumeForecast = 0
            gasVolumeForecast = 0
            waterVolumeForecast = 0

        return oilVolumeForecast, gasVolumeForecast, waterVolumeForecast

    # Function to split date from JOYN API into correct format - returns date in format of 5/17/2023 from format of 2023-05-17T00:00:00

    def splitDateFunction(badDate):
        splitDate = re.split("T", badDate)
        splitDate2 = re.split("-", splitDate[0])
        year = int(splitDate2[0])
        month = int(splitDate2[1])
        day = int(splitDate2[2])
        dateString = str(month) + "/" + str(day) + "/" + str(year)

        return dateString

    # Function to authenticate JOYN API - returns idToke used as header for authorization in other API calls

    def getIdToken():

        load_dotenv()

        login = os.getenv("JOYN_USERNAME")
        password = os.getenv("JOYN_PASSWORD")

        # User Token API
        url = "https://api.joyn.ai/common/user/token"
        # Payload for API - use JOYN crdentials
        payload = {
            "uname": str(login),
            "pwd": str(password)
        }
        # Headers for API - make sure to use content type of json
        headers = {
            "Content-Type": "application/json"
        }

        # dump payload into json format for correct format
        payloadJson = json.dumps(payload)
        response = requests.request(
            "POST", url, data=payloadJson, headers=headers)  # make request
        if response.status_code == 200:  # 200 = success
            print("Successful JOYN Authentication")
        else:
            print(response.status_code)

        results = response.json()  # get response in json format
        idToken = results["IdToken"]  # get idToken from response

        return idToken

    # Function to change product type to Oil, Gas, or Water

    def switchProductType(product):
        if product == 760010:
            product = "Gas"
        elif product == 760011:
            product = "Oil"
        elif product == 760012:
            product = "Water"
        else:
            product = "Unknown"

        return product

    # Function to get API number from JOYN ID using masterAllocationSheet

    def getApiNumber(uuid):
        if uuid in joynIdList:
            index = joynIdList.index(uuid)
            apiNumberYay = apiNumberList[index]
        else:
            apiNumberYay = "Unknown"

        return apiNumberYay

    # Function to get Well Accounting Name from JOYN ID using masterAllocationSheet
    def getName(apiNumber2):
        if apiNumber2 in apiNumberList:
            index = apiNumberList.index(apiNumber2)
            name = wellAccountingNameList[index]
        else:
            name = "Unknown"

        return name

    # Function to get State from JOYN ID using masterAllocationSheet

    def getState(apiNumber):
        if apiNumber in apiNumberList:
            index = apiNumberList.index(apiNumber)
            state = masterAllocationData["State"][index]
        else:
            state = "Unknown"

        return state

    # Function to get Client from JOYN ID using masterAllocationSheet

    def getClient(apiNumber):
        if apiNumber in apiNumberList:
            index = apiNumberList.index(apiNumber)
            client = masterAllocationData["Asset Group"][index]
        else:
            client = "Unknown"

        return client

    #### BEGIN SCRIPT #####

    idToken = getIdToken()  # get idToken from authJoyn function

    # set correct URL for Reading Data API JOYN - use idToken as header for authorization. Note the isCustom=true is required for custom entities and todate and fromdate are based on modified timestamp
    urlBase = "https://api-fdg.joyn.ai/admin/api/ReadingData?isCustom=true&entityids=15408&fromdate=2023-06-24&todate=2023-06-30&pagesize=1000&pagenumber="

    pageNumber = 1  # set page number to 1
    nextPage = True  # set nextPage to True to start while loop
    totalResults = []  # create empty list to store results

    while nextPage == True:  # loop through all pages of data
        url = urlBase + str(pageNumber)
        # makes the request to the API
        response = requests.request(
            "GET", url, headers={"Authorization": idToken})
        # Dislay status code of response - 200 = GOOD
        if response.status_code != 200:
            print(response.status_code)

        print("Length of Response: " + str(len(response.json())))

        # get response in json format and append to totalResults list
        resultsReadingType = response.json()
        totalResults.append(resultsReadingType)

        # if length of response is 0, then there is no more data to return
        if len(resultsReadingType) == 0:
            # triggers while loop to end when no more data is returned and length of response is 0
            nextPage = False

        pageNumber = pageNumber + 1  # increment page number by 1 for pagination

    # set initial variables
    readingVolume = 0
    dataSource = "di"

    # create empty dataframe to store results with correct headers for JOYN API
    headersJoynRaw = ["AssetId", "Name", "ReadingVolume",
                      "NetworkName", "Date", "Product", "Disposition"]

    headersFinal = [
        "Date",
        "Client",
        "API",
        "Well Accounting Name",
        "Oil Volume",
        "Gas Volume",
        "Water Volume",
        "Oil Sold Volume",
        "Oil Forecast",
        "Gas Forecast",
        "Water Forecast",
        "Data Source",
        "State"
    ]

    # create empty dataframe to store results with correct headers for JOYN API for both raw and final data pivot
    rawJoynTotalAssetProduction = pd.DataFrame(columns=headersJoynRaw)
    currentRunTotalAssetProductionJoyn = pd.DataFrame(columns=headersFinal)

    ## Master loop through all results from JOYN API ##
    for i in range(0, len(totalResults)):
        for j in range(0, len(totalResults[i])):
            # JOYN unquie ID for each asset
            uuidRaw = totalResults[i][j]["assetId"]
            apiNumber = getApiNumber(uuidRaw)
            wellName = getName(apiNumber)
            # reading volume for current allocation row
            readingVolume = totalResults[i][j]["Volume"]
            isDeleted = totalResults[i][j]["IsDeleted"]
            # network name for current allocation row
            networkName = totalResults[i][j]["NetworkName"]
            # reading date for current allocation row
            date = totalResults[i][j]["ReadingDate"]
            # runs splitdate() into correct format
            dateBetter = splitDateFunction(date)
            # product type for current allocation row
            productName = totalResults[i][j]["Product"]
            # runs switchProductType() to get correct product type
            newProduct = switchProductType(productName)
            # disposition for current allocation row
            disposition = totalResults[i][j]["Disposition"]

            # checking to confirm that the record is not deleted
            if isDeleted == True:
                continue

            # checking to confirm that the record is not deleted or if its oil sold volume
            if disposition == 760098 or disposition == 760101 or disposition == 760094 or disposition == 760095 or disposition == 760097:
                newProduct = "Oil Sales Volume"

            if disposition == 760096 or newProduct == "Oil Sales Volume":
                row = [apiNumber, wellName, readingVolume, networkName,
                       dateBetter, newProduct, disposition]
            else:
                continue

            # append row to dataframe
            rawJoynTotalAssetProduction.loc[len(
                rawJoynTotalAssetProduction)] = row

    # convert date column to datetime format for sorting purposes
    rawJoynTotalAssetProduction["Date"] = pd.to_datetime(
        rawJoynTotalAssetProduction["Date"])
    forecastedProductionData["Date"] = pd.to_datetime(
        forecastedProductionData["Date"])
    # sort dataframe by date for loop to get daily production
    rawTotalAssetProductionSorted = rawJoynTotalAssetProduction.sort_values(by=[
        "Date"])

    # Setting some initial variables for loop
    dailyRawProduction = np.zeros([200, 4])
    dailyRawAssetId = []
    dailyRawWellName = []
    dailyRawDate = []
    dailyClientName = []
    dailyForecastedProduction = np.zeros([200, 3])
    priorDate = -999
    wellCounter = 0
    lastIndex = 0

    # Master Loop Through the raw production data to pivot from 3 records per well per date to one record per well per date with 3 columns: Oil, Gas, Water
    for i in range(0, len(rawTotalAssetProductionSorted)):
        # get current row
        row = rawTotalAssetProductionSorted.iloc[i]
        apiNumberPivot = row["AssetId"]
        readingVolumePivot = row["ReadingVolume"]
        productTypePivot = row["Product"]
        datePivot = row["Date"]
        # oilSalesPivot = row["Oil Sales Volume"]
        wellNamePivot = getName(apiNumberPivot)
        stateName = getState(apiNumberPivot)
        clientName = getClient(apiNumberPivot)
        forecastVolumes = getForecast(
            apiNumberPivot, datePivot)

        # Loops through all the rows and pivots the data from 3 records per well per date to one record per well per date with 3 columns: Oil, Gas, Water and 1 column for Oil Sales Volume along with forecasted volumes
        if datePivot != priorDate and priorDate != -999:
            # prints all the data for the day
            for j in range(0, wellCounter):
                row = [dailyRawDate[j], dailyClientName[j], dailyRawAssetId[j], dailyRawWellName[j], dailyRawProduction[j][0], dailyRawProduction[j]
                       [1], dailyRawProduction[j][2], dailyRawProduction[j][3], dailyForecastedProduction[j][0], dailyForecastedProduction[j][1], dailyForecastedProduction[j][2], dataSource, stateName]

                currentRunTotalAssetProductionJoyn.loc[lastIndex +
                                                       j] = row
            # resets the daily variables
            dailyRawProduction = np.zeros([200, 4])
            dailyForecastedProduction = np.zeros([200, 3])
            dailyRawAssetId = []
            dailyRawWellName = []
            dailyRawDate = []
            dailyClientName = []
            wellCounter = 0  # keeping track of how many results we have on the same day
            lastIndex = lastIndex + j + 1
        # if API number is in the list, then we need to update the dailyRawProduction array
        if apiNumberPivot in dailyRawAssetId:
            index = dailyRawAssetId.index(apiNumberPivot)
            # update the dailyRawProduction array with correct index
            if productTypePivot == "Oil":
                dailyRawProduction[index][0] = readingVolumePivot
            elif productTypePivot == "Gas":
                dailyRawProduction[index][1] = readingVolumePivot
            elif productTypePivot == "Water":
                dailyRawProduction[index][2] = readingVolumePivot
            elif productTypePivot == "Oil Sales Volume":
                dailyRawProduction[index][3] = readingVolumePivot
        # if API number is not in the list, then we need to add it to the list and update the dailyRawProduction array
        else:
            dailyRawAssetId.append(apiNumberPivot)
            dailyRawWellName.append(wellNamePivot)
            cleanDatePivot = datePivot.strftime("%m/%d/%Y")
            dailyRawDate.append(cleanDatePivot)
            dailyClientName.append(clientName)
            dailyForecastedProduction[wellCounter][0] = forecastVolumes[0]
            dailyForecastedProduction[wellCounter][1] = forecastVolumes[1]
            dailyForecastedProduction[wellCounter][2] = forecastVolumes[2]
            if productTypePivot == "Oil":
                dailyRawProduction[wellCounter][0] = readingVolumePivot
            elif productTypePivot == "Gas":
                dailyRawProduction[wellCounter][1] = readingVolumePivot
            elif productTypePivot == "Water":
                dailyRawProduction[wellCounter][2] = readingVolumePivot
            elif productTypePivot == "Oil Sales Volume":
                dailyRawProduction[wellCounter][3] = readingVolumePivot
            wellCounter = wellCounter + 1
        # update priorDate to current date
        priorDate = datePivot

    # write the last day out after we have processed the last row of the last data
    for j in range(0, wellCounter):
        row = [dailyRawDate[j], dailyClientName[j], dailyRawAssetId[j], dailyRawWellName[j], dailyRawProduction[j][0], dailyRawProduction[j]
               [1], dailyRawProduction[j][2], dailyRawProduction[j][3], dailyForecastedProduction[j][0], dailyForecastedProduction[j][1], dailyForecastedProduction[j][2], dataSource, stateName]

        currentRunTotalAssetProductionJoyn.loc[lastIndex + j] = row

    # merge currentRunTotalAssetProductionJoyn into masterJoynData
    masterJoynData = mergeBIntoA(
        masterJoynData, currentRunTotalAssetProductionJoyn)

    return masterJoynData


"""
        
For this script to work, A and B MUST have the same columns and column names.
Two of those columns must be "Date" and "API". The function will merge B into A
      
"""


def mergeBIntoA(A, B):
    # convert date column to datetime format for sorting purposes
    A["Date"] = pd.to_datetime(A["Date"])
    A["Date"] = A["Date"].dt.strftime("%m/%d/%Y")
    B["Date"] = pd.to_datetime(B["Date"])
    B["Date"] = B["Date"].dt.strftime("%m/%d/%Y")
    # compare rows in B to rows in A
    for i in range(0, len(B)):
        row = B.iloc[i]
        # get index of row in A that matches row in B
        index = A.index[(A["Date"] == row["Date"]) &
                        (A["API"] == row["API"])].tolist()
        # if no index is found, then append row to A
        if len(index) > 1:
            print("Error: More than one row found")
        # if no index is found, then append row to A
        if len(index) == 0:
            A.loc[len(A)] = row
        else:
            A.iloc[index] = row

    # convert date column to datetime format for sorting purposes
    A["Date"] = pd.to_datetime(A["Date"])
    A.sort_values(by=["Date"], inplace=True, ascending=True)
    # convert date column to string format for viewing purposes
    A["Date"] = A["Date"].dt.strftime("%m/%d/%Y")

    return A
