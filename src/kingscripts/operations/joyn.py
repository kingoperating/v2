import requests
import json
import datetime as dt
import re
import pandas as pd
import os
from dotenv import load_dotenv
import numpy as np
from kingscripts.operations import combocurve
from combocurve_api_v1 import ServiceAccount


def getDailyAllocatedProduction():
    load_dotenv()

    masterAllocationData = pd.read_excel(
        r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\master\masterWellAllocation.xlsx")

    forecastedProductionData = pd.read_csv(
        r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\production\forecastWellsFinal.csv")

    joynIdList = masterAllocationData["JOYN Id"].tolist()
    apiNumberList = masterAllocationData["API"].tolist()
    wellAccountingNameList = masterAllocationData["Name in Accounting"].tolist(
    )

    # Function to split date from JOYN API into correct format - returns date in format of 5/17/2023 from format of 2023-05-17T00:00:00

    def splitDateFunction(badDate):
        splitDate = re.split("T", date)
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

    def getName(apiNumber2):
        if apiNumber2 in apiNumberList:
            index = apiNumberList.index(apiNumber2)
            name = wellAccountingNameList[index]
        else:
            name = "Unknown"

        return name

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

    def getState(apiNumber):
        if apiNumber in apiNumberList:
            index = apiNumberList.index(apiNumber)
            state = masterAllocationData["State"][index]
        else:
            state = "Unknown"

        return state

    def getClient(apiNumber):
        if apiNumber in apiNumberList:
            index = apiNumberList.index(apiNumber)
            client = masterAllocationData["Asset Group"][index]
        else:
            client = "Unknown"

        return client

    # Begin Script
    idToken = getIdToken()  # get idToken from authJoyn function

    # set correct URL for Reading Data API JOYN - use idToken as header for authorization
    urlBase = "https://api-fdg.joyn.ai/admin/api/ReadingData?isCustom=true&entityids=15408&fromdate=2023-06-22&todate=2023-06-23&pagesize=1000&pagenumber="

    pageNumber = 1  # set page number to 1
    nextPage = True
    totalResults = []  # create empty list to store results

    while nextPage == True:  # loop through all pages of data
        url = urlBase + str(pageNumber)
        # makes the request to the API

        response = requests.request(
            "GET", url, headers={"Authorization": idToken})
        if response.status_code != 200:
            print(response.status_code)

        print("Length of Response: " + str(len(response.json())))

        resultsReadingType = response.json()
        totalResults.append(resultsReadingType)

        if len(resultsReadingType) == 0:
            # triggers while loop to end when no more data is returned and length of response is 0
            nextPage = False

        pageNumber = pageNumber + 1  # increment page number by 1 for pagination

    readingVolume = 0

    headers = ["AssetId", "Name", "ReadingVolume",
               "NetworkName", "Date", "Product", "Disposition"]
    rawTotalAssetProduction = pd.DataFrame(columns=headers)

    headersFinal = ["API Number", "Well Name", "Date",
                    "Oil Volume", "Gas Volume", "Water Volume"]

    headersMerge = [
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
    finalTotalAssetProduction = pd.DataFrame(columns=headersMerge)

    dataSource = "di"

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

            if disposition == 760098 or disposition == 760101:
                oilSalesVolume = readingVolume

            if disposition == 760096 or disposition == 760101 or disposition == 760098:
                row = [apiNumber, wellName, readingVolume, networkName,
                       dateBetter, newProduct, disposition]
            else:
                continue

            # append row to dataframe
            rawTotalAssetProduction.loc[len(rawTotalAssetProduction)] = row

    # convert date column to datetime format for sorting purposes
    rawTotalAssetProduction["Date"] = pd.to_datetime(
        rawTotalAssetProduction["Date"])
    # sort dataframe by date for loop to get daily production
    rawTotalAssetProductionSorted = rawTotalAssetProduction.sort_values(by=[
        "Date"])

    # Setting some initial variables for loop
    dailyRawProduction = np.zeros([200, 3])
    dailyRawAssetId = []
    dailyRawWellName = []
    dailyRawDate = []
    priorDate = -999
    counter = 0
    lastIndex = 0

    # Master Loop Through the raw production data to pivot from 3 records per well per date to one record per well per date with 3 columns: Oil, Gas, Water
    for i in range(0, len(rawTotalAssetProductionSorted)):
        # get current row
        row = rawTotalAssetProductionSorted.iloc[i]
        apiNumberPivot = row["AssetId"]
        readingVolumePivot = row["ReadingVolume"]
        productTypePivot = row["Product"]
        datePivot = row["Date"]
        dateBetterPivot = splitDateFunction(datePivot)
        wellNamePivot = getName(apiNumberPivot)
        stateName = getState(apiNumberPivot)
        clientName = getClient(apiNumberPivot)
        forecastVolumes = getForecast(apiNumberPivot, dateBetterPivot)
        if forecastVolumes[0] != 0:
            x = 5

        if datePivot != priorDate and priorDate != -999:
            for j in range(0, counter):
                row = [dailyRawDate[j], clientName, dailyRawAssetId[j], dailyRawWellName[j], dailyRawProduction[j][0], dailyRawProduction[j]
                       [1], dailyRawProduction[j][2], 0, forecastVolumes[0], forecastVolumes[1], forecastVolumes[2], dataSource, stateName]
                # row = [dailyRawAssetId[j], dailyRawWellName[j], dailyRawDate[j],
                #        dailyRawProduction[j][0], dailyRawProduction[j][1], dailyRawProduction[j][2]]
                finalTotalAssetProduction.loc[lastIndex + j] = row

            dailyRawProduction = np.zeros([200, 3])
            dailyRawAssetId = []
            dailyRawWellName = []
            dailyRawDate = []
            counter = 0
            lastIndex = lastIndex + j + 1

        if apiNumberPivot in dailyRawAssetId:
            index = dailyRawAssetId.index(apiNumberPivot)
            if productTypePivot == "Oil":
                dailyRawProduction[index][0] = readingVolumePivot
            elif productTypePivot == "Gas":
                dailyRawProduction[index][1] = readingVolumePivot
            elif productTypePivot == "Water":
                dailyRawProduction[index][2] = readingVolumePivot

        else:
            dailyRawAssetId.append(apiNumberPivot)
            dailyRawWellName.append(wellNamePivot)
            dailyRawDate.append(datePivot)
            if productTypePivot == "Oil":
                dailyRawProduction[counter][0] = readingVolumePivot
            elif productTypePivot == "Gas":
                dailyRawProduction[counter][1] = readingVolumePivot
            elif productTypePivot == "Water":
                dailyRawProduction[counter][2] = readingVolumePivot
            counter = counter + 1

        priorDate = datePivot

    # get forecasted volume for current allocation row

        # Get State name

        # create row to append to dataframe

    return finalTotalAssetProduction
