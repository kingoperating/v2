import requests
from datetime import datetime, timedelta
import datetime as dt
import re
from dotenv import load_dotenv
import json
import pandas as pd
from combocurve_api_v1 import ComboCurveAuth
import numpy as np

"""

    Script to put Joyn data into ComboCurve - production-ready

"""


def putJoynWellProductionData(allocatedProductionMaster, serviceAccount, comboCurveApi, daysToLookback, headerData):
    
    load_dotenv()  # load enviroment variables
    
    def getApiNumber(id):
        idList = headerData["UUID"].tolist()
        if id in idList:
            index = idList.index(id)
            apiNumber = headerData["xid"][index]
        else:
            apiNumber = "Not Found"
            print("API Number Not Found")
        
        return apiNumber
    

    allocatedProduction = allocatedProductionMaster
    ## convert date to datetime and sort by date
    allocatedProduction["ReadingDate"] = pd.to_datetime(allocatedProduction["ReadingDate"])
    allocatedProduction = allocatedProduction.sort_values(by="ReadingDate")
    ## get last 30 days of data
    allocatedProduction = allocatedProduction[
        allocatedProduction["ReadingDate"] >= datetime.now() - timedelta(days=daysToLookback)
    ]

    print("Start upsert of daily well production data for update records from JOYN")

    # connect to service account
    service_account = serviceAccount
    # set API Key from enviroment variable
    api_key = comboCurveApi
    # specific Python ComboCurve authentication
    combocurve_auth = ComboCurveAuth(service_account, api_key)

    # converts API to int (removing decimals) and then back to string for JSON

    # allocatedProduction = allocatedProduction.astype({"ReadingDate": "string", "API": "string"})

    # helps when uploading to ComboCurve to check for length of data (can only send 20,000 data points at a time)
    print("Length of Total Asset Production: " + str(len(allocatedProduction)))
    
    columnsComboCurve = [
        "date",
        "chosenID",
        "oil",
        "gas",
        "water",
        "dataSource",
    ]
    
    currentRunJoynData = pd.DataFrame(columns=columnsComboCurve)
    
    # Setting some initial variables for loop
    dailyRawProduction = np.zeros([200, 3])
    dailyRawAssetId = []
    dailyRawDate = []
    priorDate = -999
    wellCounter = 0
    lastIndex = 0
    dataSource = "di"
    
    for i in range(0, len(allocatedProduction)):
        row = allocatedProduction.iloc[i]
        apiNumberPivot = row["AssetId"]
        apiNumberPivot = getApiNumber(apiNumberPivot) ## gets API Number from Well Header
        readingVolumePivot = row["ReadingVolume"]
        productTypePivot = row["Product"]
        datePivot = row["ReadingDate"]
        isDeleted = row["isDeleted"]
        dispostion = int(row["Disposition"])
        
        if dispostion == 760096 and isDeleted == False:
    
            if datePivot != priorDate and priorDate != -999:
                # prints all the data for the day
                for j in range(0, wellCounter):
                    rowBetter = [dailyRawDate[j], dailyRawAssetId[j], dailyRawProduction[j][0], dailyRawProduction[j]
                        [1], dailyRawProduction[j][2], dataSource]

                    currentRunJoynData.loc[lastIndex + j] = rowBetter
                    
                    
                # resets the daily variables
                dailyRawProduction = np.zeros([200, 4])
                dailyRawAssetId = []
                dailyRawDate = []
                wellCounter = 0  # keeping track of how many results we have on the same day
                lastIndex = lastIndex + j + 1
            
            # if API number is in the list, then we need to update the dailyRawProduction array
            if apiNumberPivot in dailyRawAssetId:
                index = dailyRawAssetId.index(apiNumberPivot)
                # update the dailyRawProduction array with correct index
                if productTypePivot == 760011:
                    dailyRawProduction[index][0] = readingVolumePivot
                elif productTypePivot == 760010:
                    dailyRawProduction[index][1] = readingVolumePivot
                elif productTypePivot == 760012:
                    dailyRawProduction[index][2] = readingVolumePivot
            else:
                dailyRawAssetId.append(apiNumberPivot)
                cleanDatePivot = datePivot.strftime("%m/%d/%Y")
                dailyRawDate.append(cleanDatePivot)
                if productTypePivot == 760011:
                    dailyRawProduction[wellCounter][0] = readingVolumePivot
                elif productTypePivot == 760010:
                    dailyRawProduction[wellCounter][1] = readingVolumePivot
                elif productTypePivot == 760012:
                    dailyRawProduction[wellCounter][2] = readingVolumePivot
     
                wellCounter = wellCounter + 1
            # update priorDate to current date
            priorDate = datePivot
        
            for j in range(0, wellCounter):
                rowBetter = [dailyRawDate[j], dailyRawAssetId[j], dailyRawProduction[j][0], dailyRawProduction[j][1],
                    dailyRawProduction[j][2], dataSource]
                
                currentRunJoynData.loc[lastIndex + j] = rowBetter
        
        ## skip all other values
        else:
            continue
    
    # drop all rows with chosenId = 123456789
    currentRunJoynData = currentRunJoynData[currentRunJoynData["chosenID"] != "123456789"]
    ##drop index column
    currentRunJoynData = currentRunJoynData.reset_index(drop=True)

    ## conver date to YYYY-MM-DD
    currentRunJoynData["date"] = pd.to_datetime(currentRunJoynData["date"])
    currentRunJoynData["date"] = currentRunJoynData["date"].dt.strftime("%Y-%m-%d")
    ## convert date to string
    currentRunJoynData["date"] = currentRunJoynData["date"].astype(str)      
    ## convert API to string
    currentRunJoynData["chosenID"] = currentRunJoynData["chosenID"].astype(str)
    
    
    totalAssetProductionJson = currentRunJoynData.to_json(
        orient="records"
    )  # converts to internal json format
    
    # loads json into format that can be sent to ComboCurve
    cleanTotalAssetProduction = json.loads(totalAssetProductionJson)

    # prints length as final check (should be less than 20,000)
    print("Length of Sliced Data: " + str(len(cleanTotalAssetProduction)))

    # sets url to daily production for combo curve for daily production
    url = "https://api.combocurve.com/v1/daily-productions"
    auth_headers = combocurve_auth.get_auth_headers()  # authenticates ComboCurve

    # put request to ComboCurve
    response = requests.put(url, headers=auth_headers, json=cleanTotalAssetProduction)

    responseCode = response.status_code  # sets response code to the current state
    responseText = response.text  # sets response text to the current state

    print("Response Code: " + str(responseCode))  # prints response code

    if (
        "successCount" in responseText
    ):  # checks if the response text contains successCount
        # finds the index of successCount
        # prints the successCount and the number of data points sent
        indexOfSuccessFail = responseText.index("successCount")
        text = responseText[indexOfSuccessFail:]
        print(text)

    print(
        "Finished PUT "
        + str(len(cleanTotalAssetProduction))
        + " Rows of New Production Data to ComboCurve from JOYN"
    )

    return text


"""
 
 Script to put Greasebook Well Production into ComboCurve - does allocation process

"""


def putGreasebookWellProductionData(
    workingDataDirectory,
    pullFromAllocation,
    serviceAccount,
    comboCurveApi,
    greasebookApi,
    daysToPull,
):
    load_dotenv()  # load enviroment variables

    pullFromAllocation = pullFromAllocation

    print(
        "Start upsert of daily well production data for the last "
        + str(daysToPull)
        + " days"
    )

    # connect to service account
    service_account = serviceAccount
    # set API Key from enviroment variable
    api_key = comboCurveApi
    # specific Python ComboCurve authentication
    combocurve_auth = ComboCurveAuth(service_account, api_key)

    print("Successful ComboCurve Authentication")

    # adding the Master Allocation List for Analysis
    workingDir = workingDataDirectory
    masterAllocationListFileName = workingDir + r"\master\masterWellAllocation.xlsx"

    masterAllocationList = pd.read_excel(masterAllocationListFileName)

    # set some date variables we will need later
    dateToday = dt.datetime.today()
    todayYear = dateToday.strftime("%Y")
    todayMonth = dateToday.strftime("%m")
    todayDay = dateToday.strftime("%d")
    dateYes = dateToday - timedelta(days=1)

    # set the interval for the API call - if True then pull EVERYTHING and will be around an hour runtime
    if pullFromAllocation == False:
        # set the interval for the API call
        numberOfDaysToPull = daysToPull
        dateThirtyDays = dateToday - timedelta(days=numberOfDaysToPull)
        dateThirtyDaysYear = dateThirtyDays.strftime("%Y")
        dateThirtyDaysMonth = dateThirtyDays.strftime("%m")
        dateThirtyDaysDay = dateThirtyDays.strftime("%d")
        productionInterval = (
            "&start="
            + dateThirtyDaysYear
            + "-"
            + dateThirtyDaysMonth
            + "-"
            + dateThirtyDaysDay
            + "&end="
        )

        # Master API call to Greasebooks
        url = (
            "https://integration.greasebook.com/api/v1/batteries/daily-production?apiKey="
            + greasebookApi
            + productionInterval
            + todayYear
            + "-"
            + todayMonth
            + "-"
            + todayDay
        )

        # make the API call
        response = requests.request(
            "GET",
            url,
        )

        responseCode = response.status_code  # sets response code to the current state

        # parse as json string
        results = response.json()
        # setting to length of results
        numEntries = len(results)

        # checks to see if the GB API call was successful
        if responseCode == 200:
            print("Sucessful Greasebook API Call")
            print(str(numEntries) + " total number of rows")
        else:
            print("The Status Code: " + str(response.status_code))

        # adds the header to string
        headerCombocurve = [
            "Date",
            "Client",
            "API",
            "Well Accounting Name",
            "Oil Volume",
            "Gas Volume",
            "Water Volume",
            "Oil Sold Volume",
            "Data Source",
        ]

        totalComboCurveAllocatedProduction = pd.DataFrame(columns=headerCombocurve)

        # set some intial variables for core logic
        welopOilVolume = 0
        welopGasVolume = 0
        welopWaterVolume = 0
        welopOilSalesVolume = 0
        welopCounter = 0
        adamsRanchCounter = 0
        adamsRanchOilVolume = 0
        adamsRanchGasVolume = 0
        adamsRanchOilVolume = 0
        adamsRanchOilSalesVolume = 0

        # Gets list of Battery id's that are clean for printing
        listOfBatteryIds = masterAllocationList["Id in Greasebooks"].tolist()
        wellNameAccountingList = masterAllocationList["Name in Accounting"].tolist()
        accountingIdList = masterAllocationList["Subaccount"].tolist()
        apiList = masterAllocationList["API"].tolist()
        allocationOilList = masterAllocationList["Allocation Ratio Oil"].tolist()
        allocationGasList = masterAllocationList["Allocation Ratio Gas"].tolist()

        apiHelpList = []

        for i in apiList:
            apiHelpList.append(str(i).split(".")[0])

        kComboCurve = 0  # variable for loop
        startingIndex = 0  # variable for loop

        lastDate = ""

        # MASTER loop that goes through each of the items in the response to build a dataframe
        for currentRow in range(numEntries - 1, 0, -1):
            row = results[currentRow]  # get row i in results
            keys = list(row.items())  # pull out the headers

            # set some intial variables for core logic
            oilDataExist = False
            gasDataExist = False
            waterDataExist = False
            oilSalesDataExist = False
            oilVolumeClean = 0
            gasVolumeClean = 0
            waterVolumeClean = 0
            oilSalesDataClean = 0

            # Loops through each exposed API variable. If it exisits - get to correct variable
            for idx, key in enumerate(keys):
                if key[0] == "batteryId":
                    batteryId = row["batteryId"]
                elif key[0] == "batteryName":
                    batteryName = row["batteryName"]
                elif key[0] == "date":
                    date = row["date"]
                # if reported, set to True, otherwise leave false
                elif key[0] == "oil":
                    oilDataExist = True
                    oilVolumeRaw = row["oil"]
                    if oilVolumeRaw == "":  # if "" means it not reported
                        oilVolumeClean = 0
                    else:
                        oilVolumeClean = oilVolumeRaw
                elif key[0] == "mcf":  # same as oil
                    gasDataExist = True
                    gasVolumeRaw = row["mcf"]
                    if gasVolumeRaw == "":
                        gasVolumeClean = 0
                    else:
                        gasVolumeClean = gasVolumeRaw
                elif key[0] == "water":  # same as oil
                    waterDataExist = True
                    waterVolumeRaw = row["water"]
                    if waterVolumeRaw == "":
                        waterVolumeClean = 0
                    else:
                        waterVolumeClean = waterVolumeRaw
                elif key[0] == "oilSales":
                    oilSalesDataExist = True
                    oilSalesDataRaw = row["oilSales"]
                    if oilSalesDataRaw == "":
                        oilSalesDataClean = 0
                    else:
                        oilSalesDataClean = oilSalesDataRaw

            # spliting date correctly
            splitDate = re.split("T", date)
            splitDate2 = re.split("-", splitDate[0])
            year = int(splitDate2[0])
            month = int(splitDate2[1])
            day = int(splitDate2[2])

            # CORE LOGIC BEGINS FOR MASTER LOOP

            # Colorado set MCF to zero
            if batteryId == 25381 or batteryId == 25382:
                gasVolumeClean = 0

            subAccountId = []  # empty list for subaccount id
            allocationRatioOil = []  # empty list for allocation ratio oil
            allocationRatioGas = []  # empty list for allocation ratio gas

            wellAccountingName = []

            # gets all the indexs that match the battery id (sometimes 1, sometimes more)
            batteryIndexId = [
                m for m, x in enumerate(listOfBatteryIds) if x == batteryId
            ]
            if batteryIndexId == []:
                continue
            # if only 1 index - then just get the subaccount id and allocation ratio
            if len(batteryIndexId) == 1:
                subAccountId = accountingIdList[batteryIndexId[0]]
                allocationRatioOil = allocationOilList[batteryIndexId[0]]
                allocationRatioGas = allocationGasList[batteryIndexId[0]]
                wellAccountingName = wellNameAccountingList[batteryIndexId[0]]
            else:  # if more than 1 index - then need to check if they are the same subaccount id
                # gets allocation for each subaccount
                for t in range(len(batteryIndexId)):
                    subAccountId.append(accountingIdList[batteryIndexId[t]])
                    allocationRatioOil.append(allocationOilList[batteryIndexId[t]])
                    allocationRatioGas.append(allocationGasList[batteryIndexId[t]])
                    wellAccountingName.append(wellNameAccountingList[batteryIndexId[t]])
            # setting the proper date format
            dateString = str(month) + "/" + str(day) + "/" + str(year)
            dateStringComboCurve = datetime.strptime(dateString, "%m/%d/%Y")

            # clears the counters if the date changes
            if lastDate != dateString:
                welopCounter = 0
                welopGasVolume = 0
                welopOilVolume = 0
                welopWaterVolume = 0
                welopOilSalesVolume = 0
                adamsRanchCounter = 0
                adamsRanchGasVolume = 0
                adamsRanchOilSalesVolume = 0
                adamsRanchOilVolume = 0
                adamsRanchWaterVolume = 0

            # Splits battery name up
            splitString = re.split("-|–", batteryName)
            # sets client name to client name from ETX/STX and GCT
            clientName = splitString[0]
            # if field name exisits - add the batteryName
            if clientName == "CWS ":
                clientName = "KOSOU"
            elif clientName == "Peak ":
                clientName = "KOEAS"
            elif clientName == "Otex ":
                clientName = "KOGCT"
            elif clientName == "Midcon ":
                clientName = "KOAND"
            elif clientName == "Wellman ":
                clientName = "KOPRM"
            elif clientName == "Wellington ":
                clientName = "WELOP"
            elif clientName == "Scurry ":
                clientName = "KOPRM"
            elif clientName == "Wyoming":
                clientName = "KOWYM"

            # CORE LOGIC FOR WELLOP
            if len(batteryIndexId) == 1:
                if batteryId != 23012:
                    newRowComboCurve = [
                        dateStringComboCurve,
                        clientName,
                        apiHelpList[batteryIndexId[0]],
                        wellAccountingName,
                        oilVolumeClean,
                        gasVolumeClean,
                        waterVolumeClean,
                        oilSalesDataClean,
                        "di",
                    ]

                    totalComboCurveAllocatedProduction.loc[
                        startingIndex + kComboCurve
                    ] = newRowComboCurve  # sets new row to combo curve
                    kComboCurve = kComboCurve + 1  # counter for combo curve

                if batteryId == 23012:
                    adamsRanchOilVolume = adamsRanchOilVolume + oilVolumeClean
                    adamsRanchGasVolume = adamsRanchGasVolume + gasVolumeClean
                    adamsRanchWaterVolume = adamsRanchWaterVolume + waterVolumeClean
                    adamsRanchOilSalesVolume = (
                        adamsRanchOilSalesVolume + oilSalesDataClean
                    )
                    adamsRanchCounter = adamsRanchCounter + 1

            elif len(batteryIndexId) > 1:
                for j in range(len(batteryIndexId)):
                    wellOilVolume = oilVolumeClean * allocationRatioOil[j] / 100
                    wellGasVolume = gasVolumeClean * allocationRatioGas[j] / 100
                    wellWaterVolume = waterVolumeClean * allocationRatioOil[j] / 100
                    wellOilSalesVolume = oilSalesDataClean * allocationRatioOil[j] / 100

                    if batteryId != 25381 and batteryId != 25382:
                        newRow = [
                            dateStringComboCurve,
                            clientName,
                            apiHelpList[batteryIndexId[j]],
                            wellAccountingName[j],
                            wellOilVolume,
                            wellGasVolume,
                            wellWaterVolume,
                            wellOilSalesVolume,
                            "di",
                        ]
                        junk = 0

                    else:
                        newRow = [
                            dateStringComboCurve,
                            clientName,
                            "0" + apiHelpList[batteryIndexId[j]],
                            wellAccountingName[j],
                            wellOilVolume,
                            wellGasVolume,
                            wellWaterVolume,
                            wellOilSalesVolume,
                            "di",
                        ]

                    totalComboCurveAllocatedProduction.loc[
                        startingIndex + kComboCurve
                    ] = newRow
                    kComboCurve = kComboCurve + 1

                if batteryId == 25381 or batteryId == 25382:
                    welopOilVolume = welopOilVolume + oilVolumeClean
                    welopGasVolume = welopGasVolume + gasVolumeClean
                    welopWaterVolume = welopWaterVolume + waterVolumeClean
                    welopOilSalesVolume = welopOilSalesVolume + oilSalesDataClean
                    welopCounter = welopCounter + 1

            lastDate = dateString
    else:
        totalComboCurveAllocatedProduction = pd.read_csv(
            r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\comboCurveAllocatedProduction.csv"
        )
        totalComboCurveAllocatedProduction = totalComboCurveAllocatedProduction.astype(
            {"API": "string"}
        )

    # converts API to int (removing decimals) and then back to string for JSON
    totalComboCurveAllocatedProduction = totalComboCurveAllocatedProduction.astype(
        {"Date": "string"}
    )

    # helps when uploading to ComboCurve to check for length of data (can only send 20,000 data points at a time)
    print(
        "Length of Total Asset Production: "
        + str(len(totalComboCurveAllocatedProduction))
    )

    # drops columns that are not needed
    totalComboCurveAllocatedProduction = totalComboCurveAllocatedProduction.drop(
        ["Well Accounting Name", "Client"], axis=1
    )

    totalComboCurveAllocatedProduction = totalComboCurveAllocatedProduction.iloc[
        0:19999
    ]

    # renames columns to match ComboCurve
    totalComboCurveAllocatedProduction.rename(
        columns={
            "Oil Volume": "oil",
            "Date": "date",
            "Gas Volume": "gas",
            "Water Volume": "water",
            "API": "chosenID",
            "Data Source": "dataSource",
            "Oil Sold Volume": "customNumber0",
        },
        inplace=True,
    )

    # exports to json for storage
    totalComboCurveAllocatedProduction.to_json(
        r".\kingoperating\data\totalAssetsProduction.json", orient="records"
    )

    totalAssetProductionJson = totalComboCurveAllocatedProduction.to_json(
        orient="records"
    )  # converts to internal json format
    # loads json into format that can be sent to ComboCurve
    cleanTotalAssetProduction = json.loads(totalAssetProductionJson)

    # prints length as final check (should be less than 20,000)
    print("Length of Sliced Data: " + str(len(cleanTotalAssetProduction)))

    # sets url to daily production for combo curve for daily production
    url = "https://api.combocurve.com/v1/daily-productions"
    auth_headers = combocurve_auth.get_auth_headers()  # authenticates ComboCurve

    # put request to ComboCurve
    response = requests.put(url, headers=auth_headers, json=cleanTotalAssetProduction)

    responseCode = response.status_code  # sets response code to the current state
    responseText = response.text  # sets response text to the current state

    print("Response Code: " + str(responseCode))  # prints response code

    if (
        "successCount" in responseText
    ):  # checks if the response text contains successCount
        # finds the index of successCount
        # prints the successCount and the number of data points sent
        indexOfSuccessFail = responseText.index("successCount")
        print(responseText[indexOfSuccessFail:])

    print("Finished Put Production Data to ComboCurve")


"""
Get the latest scenerio from a given ComboCurve project and return a pandas dataframe

"""


def getLatestScenarioOneLiner(
    workingDataDirectory, projectIdKey, scenarioIdKey, serviceAccount, comboCurveApi
):
    # FUNCTIONS
    # GET request to get Well ID to API14
    def getWellApi(wellIdComboCurve):
        authComboCurveHeaders = combocurve_auth.get_auth_headers()
        url = "https://api.combocurve.com/v1/wells/" + wellIdComboCurve
        responseApi = requests.request("GET", url, headers=authComboCurveHeaders)
        jsonStr = responseApi.text
        dataObjBetter = json.loads(jsonStr)
        return dataObjBetter["chosenID"]

    # Get the next page URL from the response headers for pagination
    def getNextPageUrlComboCurve(response_headers: dict) -> str:
        urlHeader = response_headers.get("Link", "")
        matchComboCurve = re.findall('<([^<>]+)>;rel="([^"]+)"', urlHeader)
        for linkComboCurve, rel in matchComboCurve:
            if rel == "next":
                return linkComboCurve
        return None

    def processNextPageUrlComboCurve(response_json):
        for i in range(0, len(response_json)):
            results = response_json[i]
            wellId = results["well"]
            output = results["output"]
            wellIdList.append(wellId)
            resultsList.append(output)

    # load enviroment variables
    load_dotenv()

    workingDir = workingDataDirectory
    masterAllocationListFileName = workingDir + r"\master\masterWellAllocation.xlsx"

    masterAllocationList = pd.read_excel(masterAllocationListFileName)

    # connect to service account
    service_account = serviceAccount
    # set API Key from enviroment variable
    api_key = comboCurveApi
    # specific Python ComboCurve authentication
    combocurve_auth = ComboCurveAuth(service_account, api_key)

    projectId = projectIdKey
    scenarioId = scenarioIdKey

    # This code chunk gets the  for given Scenerio
    # Call Stack - Get Econ Id

    authComboCurveHeaders = combocurve_auth.get_auth_headers()
    # URl econid
    url = (
        "https://api.combocurve.com/v1/projects/"
        + projectId
        + "/scenarios/"
        + scenarioId
        + "/econ-runs"
    )

    response = requests.request(
        "GET", url, headers=authComboCurveHeaders
    )  # GET request to pull economic ID for next query

    jsonStr = response.text  # convert to JSON string
    # pass to data object - allows for parsing
    dataObjBetter = json.loads(jsonStr)
    row = dataObjBetter[0]  # sets row equal to first string set (aka ID)
    econId = row["id"]  # set ID equal to variable

    # Reautenticated client
    authComboCurveHeaders = combocurve_auth.get_auth_headers()
    # set new url with econRunID, skipping zero

    urltwo = (
        "https://api.combocurve.com/v1/projects/"
        + projectId
        + "/scenarios/"
        + scenarioId
        + "/econ-runs/"
        + econId
        + "/one-liners"
    )

    resultsList = []
    wellIdList = []

    # boolean to check if there is a next page for pagination
    hasNextLink = True

    while hasNextLink:
        response = requests.request("GET", urltwo, headers=authComboCurveHeaders)
        urltwo = getNextPageUrlComboCurve(response.headers)
        processNextPageUrlComboCurve(response.json())
        hasNextLink = urltwo is not None

    numEntries = len(resultsList)

    apiListBest = []

    for i in range(0, len(wellIdList)):
        apiNumber = getWellApi(wellIdList[i])
        apiListBest.append(apiNumber)

    apiList = masterAllocationList["API"].tolist()
    wellIdScenariosList = masterAllocationList["Well Id"].tolist()

    headers = [
        "API",
        "Abandonment Date",
        "Gross Oil Well Head Volume",
        "Gross Gas Well Head Volume",
    ]

    comboCurveHeaders = [
        "Ad Valorem Tax",
        "After Income Tax Cash Flow",
        "Before Income Tax Cash Flow",
        "Depreciation",
        "Drip Condensate Differentials - 1",
        "Drip Condensate Differentials - 2",
        "Drip Condensate Gathering Expense",
    ]

    eurData = pd.DataFrame(columns=headers)

    for i in range(0, numEntries):
        row = resultsList[i]
        wellId = wellIdList[i]

        if wellId not in wellIdScenariosList:
            printRow = {
                "API": "0",
                "Well Name": "0",
                "Abandonment Date": "0",
                "Gross Oil Well Head Volume": "0",
                "Gross Gas Well Head Volume": "0",
            }
        else:
            wellIdIndex = wellIdScenariosList.index(wellId)
            apiNumber = apiList[wellIdIndex]
            abandonmentDate = row["abandonmentDate"]
            grossOilWellHeadVolume = row["grossOilWellHeadVolume"]
            grossGasWellHeadVolume = row["grossGasWellHeadVolume"]

            printRow = {
                "API": apiNumber,
                "Abandonment Date": abandonmentDate,
                "Gross Oil Well Head Volume": grossOilWellHeadVolume,
                "Gross Gas Well Head Volume": grossGasWellHeadVolume,
            }

        eurData.loc[len(eurData) + 1] = printRow

    return eurData


"""
This code puts the well comments into ComboCurve from Greasebook

"""


def putGreasebookWellComments(cleanJson, serviceAccount, comboCurveApi):
    # connect to service account
    service_account = serviceAccount
    # set API Key from enviroment variable
    api_key = comboCurveApi
    # specific Python ComboCurve authentication
    combocurve_auth = ComboCurveAuth(service_account, api_key)

    print("Authentication Worked")

    url = "https://api.combocurve.com/v1/daily-productions"
    auth_headers = combocurve_auth.get_auth_headers()  # authenticates ComboCurve

    # put request to ComboCurve
    response = requests.put(url, headers=auth_headers, json=cleanJson)

    responseCode = response.status_code  # sets response code to the current state
    responseText = response.text  # sets response text to the current state

    print("Response Code: " + str(responseCode))  # prints response code

    if (
        "successCount" in responseText
    ):  # checks if the response text contains successCount
        # finds the index of successCount
        # prints the successCount and the number of data points sent
        indexOfSuccessFail = responseText.index("successCount")
        print(responseText[indexOfSuccessFail:])

    print("PUT COMPLETE")


"""

This function gest the daily forecast volumes from a given ComboCurve project and forecast id

"""


def getDailyForecastVolume(projectIdKey, forecastIdKey, serviceAccount, comboCurveApi):
    # FUNCTIONS

    # get daily date list
    def getDailyDateList(startDate, finishDate):
        date_format = "%Y-%m-%d"  # Adjust the date format as per your input
        startDate = datetime.strptime(startDate, date_format)
        finishDate = datetime.strptime(finishDate, date_format)

        delta = finishDate - startDate
        daily_dates = []

        for i in range(delta.days + 1):
            day = startDate + timedelta(days=i)
            daily_dates.append(day.strftime(date_format))

        return daily_dates

    # split date function - takes unformatted date and returns formatted date
    def splitDateFunction(badDate):
        splitDate = re.split("T", badDate)

        return splitDate[0]

    # Get the next page URL from the response headers for pagination
    def getNextPageUrlComboCurve(response_headers: dict) -> str:
        urlHeader = response_headers.get("Link", "")
        matchComboCurve = re.findall('<([^<>]+)>;rel="([^"]+)"', urlHeader)
        for linkComboCurve, rel in matchComboCurve:
            if rel == "next":
                return linkComboCurve
        return None

    # process the next page URL for pagination
    def processNextPageUrlComboCurve(response_json):
        for i in range(0, len(response_json)):
            results = response_json[i]
            wellId = results["well"]
            output = results["phases"]
            wellIdList.append(wellId)
            resultsList.append(output)

    # GET request to get Well ID to API14
    def getWellApi(wellIdComboCurve):
        authComboCurveHeaders = combocurve_auth.get_auth_headers()
        url = "https://api.combocurve.com/v1/wells/" + wellIdComboCurve
        responseApi = requests.request("GET", url, headers=authComboCurveHeaders)
        jsonStr = responseApi.text
        dataObjBetter = json.loads(jsonStr)
        return dataObjBetter["chosenID"]

    print("Starting Getting Daily Forecast Volumes")

    load_dotenv()  # load enviroment variables

    # connect to service account
    service_account = serviceAccount
    # set API Key from enviroment variable
    api_key = comboCurveApi
    # specific Python ComboCurve authentication
    combocurve_auth = ComboCurveAuth(service_account, api_key)

    projectId = projectIdKey
    forecastId = forecastIdKey
    # master API list
    apiListBest = []

    # This code chunk gets the  for given Scenerio
    # Call Stack - Get Econ Id

    authComboCurveHeaders = combocurve_auth.get_auth_headers()
    # URl econid
    url = (
        "https://api.combocurve.com/v1/projects/"
        + projectId
        + "/forecasts/"
        + forecastId
        + "/daily-volumes?skip=0&take=25"
    )

    # create empty lists to be used in pagination
    resultsList = []
    wellIdList = []

    # boolean to check if there is a next page for pagination
    hasNextLink = True

    # loops through all the pages of the API call
    while hasNextLink:
        response = requests.request("GET", url, headers=authComboCurveHeaders)
        url = getNextPageUrlComboCurve(response.headers)
        processNextPageUrlComboCurve(response.json())
        hasNextLink = url is not None

    # gets the API number for each well and place in apiListBest
    for i in range(0, len(wellIdList)):
        apiNumber = getWellApi(wellIdList[i])
        apiListBest.append(apiNumber)

    # master dataframe to be used later to hold all data
    masterForecastData = pd.DataFrame()

    # Upacking resultsList - all well in forecast
    for i in range(0, len(resultsList)):
        row = resultsList[i]
        # Upacking row - all phases in well - 3
        for j in range(0, len(row)):
            phaseName = row[j]["phase"]
            rowTwo = row[j]
            seriesData = rowTwo["series"]  # getting series data
            # Upacking seriesData - all volumes in
            for k in range(0, len(seriesData)):
                seriesVolumes = seriesData[k]["volumes"]  # getting volumes
                # getting start date
                seriesStartDate = seriesData[k]["startDate"]
                seriesEndDate = seriesData[k]["endDate"]  # getting end date
                seriesStartDateClean = splitDateFunction(seriesStartDate)
                seriesEndDateClean = splitDateFunction(seriesEndDate)
                dailyDateList = getDailyDateList(
                    seriesStartDateClean, seriesEndDateClean
                )
                # creating dataframe to temp hold phase data
                df = pd.DataFrame(
                    {
                        "Date": dailyDateList,
                        "API": apiListBest[i],
                        "Volume": seriesVolumes,
                        "Phase": phaseName,
                    }
                )

                # add to master dataframe
                masterForecastData = pd.concat([masterForecastData, df])

    print("Done Getting Daily Forecast Volumes")

    return masterForecastData


"""

This function gets the rolled up scenerio data for a given project and scenerio id

"""


def getLatestScenarioMonthly(
    projectIdKey, scenarioIdKey, serviceAccount, comboCurveApi
):
    # FUNCTIONS
    # GET request to get Well ID to API14
    def getWellApi(wellIdComboCurve):
        authComboCurveHeaders = combocurve_auth.get_auth_headers()
        url = "https://api.combocurve.com/v1/wells/" + wellIdComboCurve
        responseApi = requests.request("GET", url, headers=authComboCurveHeaders)
        jsonStr = responseApi.text
        dataObjBetter = json.loads(jsonStr)
        return dataObjBetter["chosenID"]

    # Get the next page URL from the response headers for pagination
    def getNextPageUrlComboCurve(response_headers: dict) -> str:
        urlHeader = response_headers.get("Link", "")
        matchComboCurve = re.findall('<([^<>]+)>;rel="([^"]+)"', urlHeader)
        for linkComboCurve, rel in matchComboCurve:
            if rel == "next":
                return linkComboCurve
        return None

    def processNextPageUrlComboCurve(response_json):
        for i in range(0, len(response_json)):
            results = response_json[i]
            wellId = results["well"]
            output = results["output"]
            wellIdList.append(wellId)
            resultsList.append(output)

    def processNextPageUrlComboCurveResults(response_json):
        results = response_json["results"]
        resultsList.extend(results)

    # load enviroment variables
    load_dotenv()

    # connect to service account
    service_account = serviceAccount
    # set API Key from enviroment variable
    api_key = comboCurveApi
    # specific Python ComboCurve authentication
    combocurve_auth = ComboCurveAuth(service_account, api_key)

    projectId = projectIdKey
    scenarioId = scenarioIdKey

    # This code chunk gets the  for given Scenerio
    # Call Stack - Get Econ Id

    authComboCurveHeaders = combocurve_auth.get_auth_headers()
    # URl econid
    url = (
        "https://api.combocurve.com/v1/projects/"
        + projectId
        + "/scenarios/"
        + scenarioId
        + "/econ-runs"
    )

    response = requests.request(
        "GET", url, headers=authComboCurveHeaders
    )  # GET request to pull economic ID for next query

    jsonStr = response.text  # convert to JSON string
    dataObjBetter = json.loads(jsonStr)  # pass to data object - allows for parsing
    row = dataObjBetter[0]  # sets row equal to first string set (aka ID)
    econId = row["id"]  # set ID equal to variable

    print(econId)  # check that varaible is passed correctly

    # Reautenticated client
    auth_headers = combocurve_auth.get_auth_headers()
    # Set new url parsed with updated econID
    urlone = (
        "https://api.combocurve.com/v1/projects/"
        + projectId
        + "/scenarios/"
        + scenarioId
        + "/econ-runs/"
        + econId
        + "/monthly-exports"
    )

    response = requests.request(
        "POST", urlone, headers=auth_headers
    )  # runs POST request

    # same as above chunk, parses JSON string and pull outs econRunID to be passed in next GET request
    jsonStr = response.text
    dataObjEconRunId = json.loads(jsonStr)
    row = dataObjEconRunId
    econRunId = row["id"]
    print(econRunId)

    # Reautenticated client
    auth_headers = combocurve_auth.get_auth_headers()
    # set new url with econRunID, skipping zero

    urltwo = (
        "https://api.combocurve.com/v1/projects/"
        + projectId
        + "/scenarios/"
        + scenarioId
        + "/econ-runs/"
        + econId
        + "/monthly-exports/"
        + econRunId
        + "?take=200"
    )

    resultsList = []
    wellIdList = []

    # boolean to check if there is a next page for pagination
    hasNextLink = True

    while hasNextLink:
        response = requests.request("GET", urltwo, headers=authComboCurveHeaders)
        urltwo = getNextPageUrlComboCurve(response.headers)
        processNextPageUrlComboCurveResults(response.json())
        hasNextLink = urltwo is not None

    numEntries = len(resultsList)
    print(numEntries)

    # lists for each of the columns I need rolled up
    grossOilSalesVolume = []
    grossGasSalesVolume = []
    grossNglSalesVolume = []
    grossWaterWellHeadVolume = []
    grossFixedCost = []
    netOilSalesVolume = []
    netGasSalesVolume = []
    netNglSalesVolume = []
    oilRevenueTable = []
    gasRevenueTable = []
    nglRevenueTable = []
    totalNetRevenueTable = []
    totalGrossFixedExpenseTable = []
    totalGrossVariableExpenseTable = []
    totalOilVariableExpenseTable = []
    totalGasVariableExpenseTable = []
    totalNglVariableExpenseTable = []
    netIncomeTable = []
    totalCapexTable = []
    beforeIncomeTaxCashFlowTable = []
    totalTaxTable = []
    dateTable = []
    adValoremTaxTable = []
    oilSeveranceTaxTable = []
    gasSeveranceTaxTable = []
    nglSeveranceTaxTable = []
    waterDisposalExpenseTable = []

    # Setting row, wellId and date to correct values
    for i in range(0, numEntries):
        row = resultsList[i]
        wellId = row["well"]
        output = row["output"]
        date = row["date"]
        # getting the total variables casted as floats for addition
        totalGrossOilSalesVolume = float(output["grossOilSalesVolume"])
        totalGrossGasSalesVolume = float(output["grossGasSalesVolume"])
        totalGrossNglSalesVolume = float(output["grossNglSalesVolume"])
        totalGrossWaterWellHeadVolume = float(output["grossWaterWellHeadVolume"])
        totalGrossFixedExpense = float(output["totalFixedExpense"])
        totalNetOilSalesVolume = float(output["netOilSalesVolume"])
        totalNetGasSalesVolume = float(output["netGasSalesVolume"])
        totalNetNglSalesVolume = float(output["netNglSalesVolume"])
        adValoremTax = float(output["adValoremTax"])
        oilSeveranceTax = float(output["oilSeveranceTax"])
        gasSeveranceTax = float(output["gasSeveranceTax"])
        nglSeveranceTax = float(output["nglSeveranceTax"])
        totalGrossVariableExpense = float(output["totalVariableExpense"])
        totalOilVariableExpense = float(output["totalOilVariableExpense"])
        totalGasVariableExpense = float(output["totalGasVariableExpense"])
        totalNglVariableExpense = float(output["totalNglVariableExpense"])
        totalWaterDisposalExpense = float(output["waterDisposal"])
        oilRev = float(output["oilRevenue"])
        gasRev = float(output["gasRevenue"])
        nglRev = float(output["nglRevenue"])
        totalNetRevenue = float(output["totalRevenue"])
        netIncome = float(output["netIncome"])
        totalTaxSum = (
            float(output["totalSeveranceTax"])
            + float(output["adValoremTax"])
            + float(output["totalProductionTax"])
        )

        # loop to confirm new well and same date
        for j in range(i + 1, numEntries):
            row2 = resultsList[j]
            wellId2 = row2["well"]
            date2 = row2["date"]
            # check to make sure wellID ISNT the same and the date is, then calculate all date
            if (wellId2 != wellId) and (date2 == date):
                output = row2["output"]
                totalGrossOilSalesVolume = totalGrossOilSalesVolume + float(
                    output["grossOilSalesVolume"]
                )
                totalGrossGasSalesVolume = totalGrossGasSalesVolume + float(
                    output["grossGasSalesVolume"]
                )
                totalGrossNglSalesVolume = totalGrossNglSalesVolume + float(
                    output["grossNglSalesVolume"]
                )
                totalGrossWaterWellHeadVolume = totalGrossWaterWellHeadVolume + float(
                    output["grossWaterWellHeadVolume"]
                )
                totalGrossFixedExpense = totalGrossFixedExpense + float(
                    output["totalFixedExpense"]
                )
                totalNetOilSalesVolume = totalNetOilSalesVolume + float(
                    output["netOilSalesVolume"]
                )
                totalNetGasSalesVolume = totalNetGasSalesVolume + float(
                    output["netGasSalesVolume"]
                )
                totalNetNglSalesVolume = totalNetNglSalesVolume + float(
                    output["netNglSalesVolume"]
                )
                totalGrossVariableExpense = totalGrossVariableExpense + float(
                    output["totalVariableExpense"]
                )
                totalOilVariableExpense = totalOilVariableExpense + float(
                    output["totalOilVariableExpense"]
                )
                totalGasVariableExpense = totalGasVariableExpense + float(
                    output["totalGasVariableExpense"]
                )
                totalNglVariableExpense = totalNglVariableExpense + float(
                    output["totalNglVariableExpense"]
                )
                adValoremTax = adValoremTax + float(output["adValoremTax"])
                oilSeveranceTax = oilSeveranceTax + float(output["oilSeveranceTax"])
                gasSeveranceTax = gasSeveranceTax + float(output["gasSeveranceTax"])
                nglSeveranceTax = nglSeveranceTax + float(output["nglSeveranceTax"])
                oilRev = oilRev + float(output["oilRevenue"])
                gasRev = gasRev + float(output["gasRevenue"])
                nglRev = nglRev + float(output["nglRevenue"])
                totalWaterDisposalExpense = totalWaterDisposalExpense + float(
                    output["waterDisposal"]
                )
                totalNetRevenue = totalNetRevenue + float(output["totalRevenue"])
                netIncome = netIncome + float(output["netIncome"])
                totalTaxSum = (
                    totalTaxSum
                    + float(output["totalSeveranceTax"])
                    + float(output["adValoremTax"])
                    + float(output["totalProductionTax"])
                )
        # counter to confirm same date
        dateCount = dateTable.count(date)
        # if dateCount is 0, then add each new summed variable to new list
        if dateCount == 0:
            dateTable.append(date)
            grossOilSalesVolume.append(totalGrossOilSalesVolume)
            grossGasSalesVolume.append(totalGrossGasSalesVolume)
            grossNglSalesVolume.append(totalGrossNglSalesVolume)
            grossWaterWellHeadVolume.append(totalGrossWaterWellHeadVolume)
            totalGrossFixedExpenseTable.append(totalGrossFixedExpense)
            totalNetRevenueTable.append(totalNetRevenue)
            oilRevenueTable.append(oilRev)
            gasRevenueTable.append(gasRev)
            nglRevenueTable.append(nglRev)
            netIncomeTable.append(netIncome)
            totalTaxTable.append(totalTaxSum)
            netOilSalesVolume.append(totalNetOilSalesVolume)
            netGasSalesVolume.append(totalNetGasSalesVolume)
            netNglSalesVolume.append(totalNetNglSalesVolume)
            totalGrossVariableExpenseTable.append(totalGrossVariableExpense)
            totalOilVariableExpenseTable.append(totalOilVariableExpense)
            totalGasVariableExpenseTable.append(totalGasVariableExpense)
            totalNglVariableExpenseTable.append(totalNglVariableExpense)
            adValoremTaxTable.append(adValoremTax)
            oilSeveranceTaxTable.append(oilSeveranceTax)
            gasSeveranceTaxTable.append(gasSeveranceTax)
            nglSeveranceTaxTable.append(nglSeveranceTax)
            waterDisposalExpenseTable.append(totalWaterDisposalExpense)

    combinedLists = list(
        zip(
            dateTable,
            grossOilSalesVolume,
            grossGasSalesVolume,
            grossNglSalesVolume,
            grossWaterWellHeadVolume,
            totalGrossFixedExpenseTable,
            netOilSalesVolume,
            netGasSalesVolume,
            netNglSalesVolume,
            oilRevenueTable,
            gasRevenueTable,
            nglRevenueTable,
            totalNetRevenueTable,
            netIncomeTable,
            totalTaxTable,
            totalGrossVariableExpenseTable,
            totalOilVariableExpenseTable,
            totalGasVariableExpenseTable,
            totalNglVariableExpenseTable,
            adValoremTaxTable,
            oilSeveranceTaxTable,
            gasSeveranceTaxTable,
            nglSeveranceTaxTable,
            waterDisposalExpenseTable,
        )
    )

    scenerioDataTable = pd.DataFrame(
        combinedLists,
        columns=[
            "Date",
            "Gross Oil Sales Volume",
            "Gross Gas Sales Volume",
            "Gross NGL Sales Volume",
            "Gross Water Well Head Volume",
            "Total Gross Fixed Expense",
            "Net Oil Sales Volume",
            "Net Gas Sales Volume",
            "Net NGL Sales Volume",
            "Oil Revenue",
            "Gas Revenue",
            "NGL Revenue",
            "Total Net Revenue",
            "Net Income",
            "Total Tax",
            "Total Gross Variable Expense",
            "Total Oil Variable Expense",
            "Total Gas Variable Expense",
            "Total NGL Variable Expense",
            "Ad Valorem Tax",
            "Oil Severance Tax",
            "Gas Severance Tax",
            "NGL Severance Tax",
            "Water Disposal Expense",
        ],
    )

    scenerioDataTable["Date"] = pd.to_datetime(scenerioDataTable["Date"])
    scenerioDataTable = scenerioDataTable.sort_values(by="Date")

    return scenerioDataTable


"""

This function converts a ComboCurve getLatestScenarioMonthly single well economic dataframe into a CSV export ready for import to CrestFP

"""


def ccScenarioToCrestFpSingleWell(
    comboCurveScenarioData,
    nglYield,
    gasBtuFactor,
    gasShrinkFactor,
    oilPricePercent,
    gasPricePercent,
    nglPricePercent,
    oilVariableCost,
    gasVariableCost,
    nglVariableCost,
    waterVariableCost,
    state,
    capex,
):
    ## SET STATE SPECIFIC VARIABLES
    if state == "texas":
        oilSevPercentPrint = 0.046
        gasSevPercentPrint = 0.075
        nglSevPercentPrint = 0.045
        adValoremPercentPrint = 0.025

    if state == "wyoming":
        oilSevPercentPrint = 0.0606
        gasSevPercentPrint = 0.0606
        nglSevPercentPrint = 0.0606
        adValoremPercentPrint = 0.07

    # Set variables lists needed for crestFP
    grossOilSalesVolumeList = comboCurveScenarioData["Gross Oil Sales Volume"].tolist()
    grossGasSalesVolumeList = comboCurveScenarioData["Gross Gas Sales Volume"].tolist()
    grossNglSalesVolumeList = comboCurveScenarioData["Gross NGL Sales Volume"].tolist()
    grossWaterWellHeadVolumeList = comboCurveScenarioData[
        "Gross Water Well Head Volume"
    ].tolist()
    totalGrossFixedExpenseTableList = comboCurveScenarioData[
        "Total Gross Fixed Expense"
    ].tolist()

    # setting correct columns needed for crestFP
    columns = [
        "Gross DC&E",
        "Gross Oil (MBO)",
        "Gross Gas (MMCF)",
        "Gross NGL",
        "NGL Yield",
        "Gas Shrink (%)",
        "Gas BTU Factor",
        "Gross Water (MBO)",
        "Oil Price %",
        "Oil Deduct",
        "Oil Severance %",
        "Gas Price %",
        "Gas Price Deduct",
        "Gas Severance %",
        "NGL Price %",
        "NGL Deduct",
        "NGL Severance %",
        "Ad Valorem %",
        "Gross Fixed Costs",
        "Gross Other Capital",
        "Oil Variable LOE",
        "Gas Variable LOE",
        "NGL Variable LOE",
        "Water Variable LOE",
    ]

    # setting correct datarframe size for crestFP
    crestFpOutput = pd.DataFrame(
        index=range(0, len(comboCurveScenarioData)), columns=columns
    )

    ## LOOP THROUGH EACH ROW AND SET VARIABLES
    for i in range(0, len(comboCurveScenarioData)):
        ## sets CAPEX correctly
        if i == 0:
            capex = capex
        else:
            capex = 0

        grossOilSalesVolume = grossOilSalesVolumeList[i]
        grossGasSalesVolume = grossGasSalesVolumeList[i]
        grossNglSalesVolume = grossNglSalesVolumeList[i]
        nglYieldPrint = nglYield
        gasShrinkPrint = gasShrinkFactor
        gasBtuFactorPrint = gasBtuFactor
        grossWaterWellHeadVolume = grossWaterWellHeadVolumeList[i]
        oilPricePercentPrint = oilPricePercent
        oilDeduct = 0
        gasPricePercentPrint = gasPricePercent
        gasDeduct = 0
        nglPricePercentPrint = nglPricePercent
        nglDeduct = 0
        grossFixedCost = totalGrossFixedExpenseTableList[i]
        grossOtherCapital = 0
        oilVariableLoe = oilVariableCost
        gasVariableLoe = gasVariableCost
        nglVariableLoe = nglVariableCost
        waterVariableLoe = waterVariableCost

        # skips rows that have no production
        if (
            grossOilSalesVolume == 0
            and grossGasSalesVolume == 0
            and grossNglSalesVolume == 0
        ):
            continue
        else:  # print the row
            row = [
                capex,
                grossOilSalesVolume,
                grossGasSalesVolume,
                grossNglSalesVolume,
                nglYieldPrint,
                gasShrinkPrint,
                gasBtuFactorPrint,
                grossWaterWellHeadVolume,
                oilPricePercentPrint,
                oilDeduct,
                oilSevPercentPrint,
                gasPricePercentPrint,
                gasDeduct,
                gasSevPercentPrint,
                nglPricePercentPrint,
                nglDeduct,
                nglSevPercentPrint,
                adValoremPercentPrint,
                grossFixedCost,
                grossOtherCapital,
                oilVariableLoe,
                gasVariableLoe,
                nglVariableLoe,
                waterVariableLoe,
            ]

            crestFpOutput.loc[i] = row

    # drop all rows that are empty
    crestFpOutput.dropna(axis=0, how="all", inplace=True)

    # set empty list for storing month list
    monthList = []
    # create month list for printing
    for i in range(0, len(crestFpOutput)):
        monthList.append("Month " + str(i + 1))
    # insert month list into dataframe
    crestFpOutput.insert(0, "Month", monthList)

    newHeaders = [
        "Name",
        "CapexIn",
        "GrossOilIn",
        "GrossGasIn",
        "GrossNGLIn",
        "NGLYieldIn",
        "GasShrinkIn",
        "GasBTUIn",
        "GrossWaterIn",
        "OilPricePercentIn",
        "OilDeductIn",
        "OilSeveranceIn",
        "GasPricePercentIn",
        "GasPriceDeductIn",
        "GasSeveranceIn",
        "NGLPricePercentIn",
        "NGLDeductIn",
        "NGLSeveranceIn",
        "AdValoremIn",
        "GrossFixedCostsIn",
        "GrossOtherCapitalIn",
        "OilVariableIn",
        "GasVariableIn",
        "NGLVariableIn",
        "WaterVariableIn",
    ]

    crestFpOutputShifted = crestFpOutput.shift(2, axis=0)
    crestFpOutputShifted = crestFpOutputShifted.shift(1, axis=1)
    crestFpOutputShifted = crestFpOutputShifted.fillna("0")
    
    return crestFpOutputShifted  # return dataframe


"""

This function converts a ComboCurve getLatestScenarioMonthly PDP dataframe into a CSV export ready for import to CrestFP

"""


def ccScenarioToCrestFpPdp(
    comboCurveScenarioData,
    nglYield,
    gasBtuFactor,
    gasShrinkFactor,
    oilPricePercent,
    gasPricePercent,
    nglPricePercent,
):
    # Set variables lists needed for crestFP
    netOilSalesVolumeList = comboCurveScenarioData["Net Oil Sales Volume"].tolist()
    netGasSalesVolumeList = comboCurveScenarioData["Net Gas Sales Volume"].tolist()
    netNglSalesVolumeList = comboCurveScenarioData["Net NGL Sales Volume"].tolist()
    grossWaterWellHeadVolumeList = comboCurveScenarioData[
        "Gross Water Well Head Volume"
    ].tolist()
    oilRevenueTableList = comboCurveScenarioData["Oil Revenue"].tolist()
    gasRevenueTableList = comboCurveScenarioData["Gas Revenue"].tolist()
    nglRevenueTableList = comboCurveScenarioData["NGL Revenue"].tolist()
    totalGrossFixedExpenseTableList = comboCurveScenarioData[
        "Total Gross Fixed Expense"
    ].tolist()
    totalOilVariableExpenseTableList = comboCurveScenarioData[
        "Total Oil Variable Expense"
    ].tolist()
    totalGasVariableExpenseTableList = comboCurveScenarioData[
        "Total Gas Variable Expense"
    ].tolist()
    totalNglVariableExpenseTableList = comboCurveScenarioData[
        "Total NGL Variable Expense"
    ].tolist()
    totalAdValoremTaxTableList = comboCurveScenarioData["Ad Valorem Tax"].tolist()
    totalOilSeveranceTaxTableList = comboCurveScenarioData["Oil Severance Tax"].tolist()
    totalGasSeveranceTaxTableList = comboCurveScenarioData["Gas Severance Tax"].tolist()
    totalNglSeveranceTaxTableList = comboCurveScenarioData["NGL Severance Tax"].tolist()
    totalWaterDisposalExpenseTableList = comboCurveScenarioData[
        "Water Disposal Expense"
    ].tolist()

    # setting correct columns needed for crestFP
    columns = [
        "Net Abandon",
        "Net Oil (MBO)",
        "Net Gas (MMCF)",
        "Net NGL",
        "NGL Yield",
        "Gas Shrink (%)",
        "Gas BTU Factor",
        "Gross Water (MBO)",
        "Oil Price %",
        "Oil Deduct",
        "Oil Severance %",
        "Gas Price %",
        "Gas Price Deduct",
        "Gas Severance %",
        "NGL Price %",
        "NGL Deduct",
        "NGL Severance %",
        "Ad Valorem %",
        "Net Fixed Costs",
        "Net Other Capital",
        "Oil Variable LOE",
        "Gas Variable LOE",
        "NGL Variable LOE",
        "Water Variable LOE",
    ]

    # setting correct datarframe size for crestFP
    crestFpOutput = pd.DataFrame(
        index=range(0, len(comboCurveScenarioData)), columns=columns
    )

    ## LOOP THROUGH EACH ROW AND SET VARIABLES
    for i in range(0, len(comboCurveScenarioData)):
        netAband = 0
        netOilSalesVolume = netOilSalesVolumeList[i]
        netGasSalesVolume = netGasSalesVolumeList[i]
        netNglSalesVolume = netNglSalesVolumeList[i]
        nglYieldPrint = nglYield
        gasShrinkPrint = gasShrinkFactor
        gasBtuFactorPrint = gasBtuFactor
        grossWaterWellHeadVolume = grossWaterWellHeadVolumeList[i]
        oilPricePercentPrint = oilPricePercent
        oilDeduct = 0
        gasPricePercentPrint = gasPricePercent
        gasDeduct = 0
        nglPricePercentPrint = nglPricePercent
        nglDeduct = 0
        grossFixedCost = totalGrossFixedExpenseTableList[i]
        grossOtherCapital = 0

        ## CALCULATE AVERAGE SEVERANCE TAX PERCENTAGES / AD VALOREM

        if oilRevenueTableList[i] == 0:
            oilSevPercentAvg = 0
        else:
            oilSevPercentAvg = totalOilSeveranceTaxTableList[i] / oilRevenueTableList[i]

        if gasRevenueTableList[i] == 0:
            gasSevPercentAvg = 0
        else:
            gasSevPercentAvg = totalGasSeveranceTaxTableList[i] / gasRevenueTableList[i]

        if nglRevenueTableList[i] == 0:
            nglSevPercentAvg = 0
        else:
            nglSevPercentAvg = totalNglSeveranceTaxTableList[i] / nglRevenueTableList[i]

        leftoverOilRev = oilRevenueTableList[i] - totalOilSeveranceTaxTableList[i]
        leftoverGasRev = gasRevenueTableList[i] - totalGasSeveranceTaxTableList[i]
        leftoverNglRev = nglRevenueTableList[i] - totalNglSeveranceTaxTableList[i]
        sumOfLeftoverRev = leftoverOilRev + leftoverGasRev + leftoverNglRev

        if sumOfLeftoverRev == 0:
            adValoremPercentAvg = 0
        else:
            adValoremPercentAvg = totalAdValoremTaxTableList[i] / sumOfLeftoverRev

        ## CALCULATE VARIABLE OIL GAS WATER LOE

        if netOilSalesVolume == 0:
            oilVariableLoe = 0
        else:
            oilVariableLoe = totalOilVariableExpenseTableList[i] / netOilSalesVolume

        if netGasSalesVolume == 0:
            gasVariableLoe = 0
        else:
            gasVariableLoe = totalGasVariableExpenseTableList[i] / netGasSalesVolume

        if netNglSalesVolume == 0:
            nglVariableLoe = 0
        else:
            nglVariableLoe = totalNglVariableExpenseTableList[i] / netNglSalesVolume

        if grossWaterWellHeadVolume == 0:
            waterVariableLoe = 0
        else:
            waterVariableLoe = (
                totalWaterDisposalExpenseTableList[i] / grossWaterWellHeadVolume
            )

        row = [
            netAband,
            netOilSalesVolume,
            netGasSalesVolume,
            netNglSalesVolume,
            nglYieldPrint,
            gasShrinkPrint,
            gasBtuFactorPrint,
            grossWaterWellHeadVolume,
            oilPricePercentPrint,
            oilDeduct,
            oilSevPercentAvg,
            gasPricePercentPrint,
            gasDeduct,
            gasSevPercentAvg,
            nglPricePercentPrint,
            nglDeduct,
            nglSevPercentAvg,
            adValoremPercentAvg,
            grossFixedCost,
            grossOtherCapital,
            oilVariableLoe,
            gasVariableLoe,
            nglVariableLoe,
            waterVariableLoe,
        ]

        crestFpOutput.loc[i] = row

    # drop all rows that are empty
    crestFpOutput.dropna(axis=0, how="all", inplace=True)

    return crestFpOutput  # return dataframe