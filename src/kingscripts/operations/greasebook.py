import requests
import os
from datetime import datetime, timedelta
import datetime as dt
import re
import pandas as pd
import numpy as np
import json
import requests
from datetime import timedelta
import datetime as dt
import re
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path
from datetime import datetime


def getBatteryProductionData(workingDataDirectory, pullProd, days, greasebookApi):

    print("Begin Pulling Battery Production Data From Greasebook")

    fullProductionPull = pullProd
    numberOfDaysToPull = days

    workingDir = workingDataDirectory
    fileNameAssetProduction = workingDir + \
        r"\totalAssetsProduction.csv"
    fileNameMasterBatteryList = workingDir + \
        r"\master\masterBatteryList.csv"

    # adding the Master Battery List for Analysis
    masterBatteryList = pd.read_csv(
        fileNameMasterBatteryList, encoding="windows-1252"
    )

    # set some date variables we will need later
    dateToday = dt.datetime.today()
    todayYear = dateToday.strftime("%Y")
    todayMonth = dateToday.strftime("%m")
    todayDay = dateToday.strftime("%d")
    dateYes = dateToday - timedelta(days=1)

    # Set production interval based on boolen
    if fullProductionPull == True:
        productionInterval = "&start=2021-03-01&end="
    else:
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

    # Oil Sold Accouting Check URL
    date100DaysAgo = dateToday - timedelta(days=100)
    date100Year = date100DaysAgo.strftime("%Y")
    date100Month = date100DaysAgo.strftime("%m")
    date100Day = date100DaysAgo.strftime("%d")
    productionInterval = (
        "&start="
        + date100Year
        + "-"
        + date100Month
        + "-"
        + date100Day
        + "&end="
    )

    urlAccounting = (
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

    if responseCode == 200:
        print("Sucessful Greasebook API Call")
        print(str(numEntries) + " total number of rows")
    else:
        print("The Status Code: " + str(response.status_code))

    # checks if we need to pull all the data or just last specificed
    if fullProductionPull == False:
        # Opening Master CSV for total asset production
        totalAssetProduction = pd.read_csv(fileNameAssetProduction)
    else:
        headerList = [
            "Date",
            "Client",
            "Battery Name",
            "Oil Volume",
            "Gas Volume",
            "Water Volume",
            "Last 7 Day Oil Average",
            "Last 14 Day Oil Average",
            "Last 7 Day Gas Average",
            "Last 14 Day Gas Average",
        ]
        totalAssetProduction = pd.DataFrame(
            0, index=np.arange(numEntries - 1), columns=headerList
        )

    # a bunch of variables the below loop needs
    wellIdList = []
    wellNameList = []
    runningTotalOil = []
    runningTotalGas = []
    numberOfDaysBattery = []
    wellOilVolumeOneDayAgo = np.zeros([200], dtype=object)
    wellGasVolumeOneDayAgo = np.zeros([200], dtype=object)
    avgOilList = []
    avgGasList = []
    fourteenDayOilData = np.zeros([200, 14], dtype=float)
    sevenDayOilData = np.zeros([200, 7], dtype=float)
    fourteenDayGasData = np.zeros([200, 14], dtype=float)
    sevenDayGasData = np.zeros([200, 7], dtype=float)
    batteryIdCounterFourteen = np.zeros([200], dtype=int)
    batteryIdCounterSeven = np.zeros([200], dtype=int)
    rollingFourteenDayPerWellOil = np.zeros([200], dtype=float)
    rollingFourteenDayPerWellGas = np.zeros([200], dtype=float)
    wellIdOilSoldList = []
    wellVolumeOilSoldList = []
    gotDayData = np.full([200], False)
    totalOilVolume = 0
    totalGasVolume = 0
    totalWaterVolume = 0
    yesTotalOilVolume = 0
    yesTotalGasVolume = 0
    twoDayOilVolume = 0
    twoDayGasVolume = 0
    threeDayOilVolume = 0
    threeDayGasVolume = 0
    thrityDayOilSalesVolume = 0
    lastWeekTotalOilVolume = 0
    lastWeekTotalGasVolume = 0
    monthlyOilSales = 0

    # Convert all dates to str for comparison rollup
    todayYear = int(dateToday.strftime("%Y"))
    todayMonth = int(dateToday.strftime("%m"))
    todayDay = int(dateToday.strftime("%d"))

    dateYesterday = dateToday - timedelta(days=1)
    dateTwoDaysAgo = dateToday - timedelta(days=2)
    dateThreeDaysAgo = dateToday - timedelta(days=3)
    dateLastWeek = dateToday - timedelta(days=8)

    yesYear = int(dateYesterday.strftime("%Y"))
    yesMonth = int(dateYesterday.strftime("%m"))
    yesDay = int(dateYesterday.strftime("%d"))

    twoDayYear = int(dateTwoDaysAgo.strftime("%Y"))
    twoDayMonth = int(dateTwoDaysAgo.strftime("%m"))
    twoDayDay = int(dateTwoDaysAgo.strftime("%d"))

    threeDayYear = int(dateThreeDaysAgo.strftime("%Y"))
    threeDayMonth = int(dateThreeDaysAgo.strftime("%m"))
    threeDayDay = int(dateThreeDaysAgo.strftime("%d"))

    lastWeekYear = int(dateLastWeek.strftime("%Y"))
    lastWeekMonth = int(dateLastWeek.strftime("%m"))
    lastWeekDay = int(dateLastWeek.strftime("%d"))

    # if not pulling all of production - then just get the list of dates to parse
    if fullProductionPull == False:
        # gets list of dates
        listOfDates = totalAssetProduction["Date"].to_list()
        # finds out what date is last
        lastRow = totalAssetProduction.iloc[len(totalAssetProduction) - 1]
        dateOfLastRow = lastRow["Date"]
        splitDate = re.split("/", str(dateOfLastRow))  # splits date correct
        day = int(splitDate[1])  # gets the correct day
        month = int(splitDate[0])  # gets the correct month
        year = int(splitDate[2])  # gets the correct
        referenceTime15Day = dt.date(year, month, day) - \
            timedelta(days=15)  # creates a reference time
        dateOfInterest = referenceTime15Day.strftime(
            "%#m/%#d/%Y")  # converts to string
        startingIndex = listOfDates.index(
            dateOfInterest)  # create index surrounding
    else:
        startingIndex = 0
        referenceTime15Day = dt.date(2021, 3, 1)

    # Gets list of Battery id's that are clean for printing
    listOfBatteryIds = masterBatteryList["Id"].tolist()
    goodBatteryNames = masterBatteryList["Pretty Battery Name"].tolist()
    pumperNames = masterBatteryList["Pumper"].tolist()

    j = 0

    priorDay = -999

    initalSizeOfTotalAssetProduction = len(totalAssetProduction)

    # MASTER loop that goes through each of the items in the response
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

        if priorDay == day or priorDay == -999:
            newDay = False
        else:
            newDay = True

        # CORE LOGIC BEGINS FOR MASTER LOOP

        # checks to see if last day had an entry for every well - if not fixes it to include
        if newDay == True:
            for counter in range(0, len(wellIdList)):
                if gotDayData[counter] != True:
                    fourteenDayOilData[counter][batteryIdCounterFourteen[counter]] = 0
                    fourteenDayGasData[counter][batteryIdCounterFourteen[counter]] = 0
                    sevenDayOilData[counter][batteryIdCounterSeven[counter]] = 0
                    sevenDayGasData[counter][batteryIdCounterSeven[counter]] = 0
                    lastFourteenDayTotalOil = sum(
                        fourteenDayOilData[counter]) / (14)
                    lastFourteenDayTotalGas = sum(
                        fourteenDayGasData[counter]) / (14)
                    rollingFourteenDayPerWellOil[counter] = lastFourteenDayTotalOil
                    rollingFourteenDayPerWellGas[counter] = lastFourteenDayTotalGas
                    if year == yesYear and month == yesMonth and day == yesDay:
                        wellOilVolumeOneDayAgo[counter] = "No Data Reported"
                        wellGasVolumeOneDayAgo[counter] = "No Data Reported"
                    if batteryIdCounterFourteen[counter] < 13:
                        batteryIdCounterFourteen[counter] = batteryIdCounterFourteen[counter] + 1
                    else:
                        batteryIdCounterFourteen[counter] = 0
                    if batteryIdCounterSeven[counter] < 6:
                        batteryIdCounterSeven[counter] = batteryIdCounterSeven[counter] + 1
                    else:
                        batteryIdCounterSeven[counter] = 0

        # resets gotDayData to False as we loop the current day
        if newDay == True:
            gotDayData = np.full((200), False)
            newDay = False

        # Colorado set MCF to zero
        if batteryId == 25381 or batteryId == 25382:
            gasVolumeClean = 0

        if oilSalesDataClean != 0:
            thrityDayOilSalesVolume = thrityDayOilSalesVolume + oilSalesDataClean

            if year == todayYear and month == todayMonth:
                monthlyOilSales = monthlyOilSales + oilSalesDataClean

                if batteryId in wellIdOilSoldList:
                    index = wellIdOilSoldList.index(batteryId)
                    wellVolumeOilSoldList[index] = float(
                        wellVolumeOilSoldList[index]) + oilSalesDataClean
                else:
                    wellIdOilSoldList.append(batteryId)
                    index = wellIdOilSoldList.index(batteryId)
                    wellVolumeOilSoldList.append(oilSalesDataClean)

        if batteryId in wellIdList:  # builds a list of all battery ID's with data
            index = wellIdList.index(batteryId)
            # running total of oil/gas and number of reporeted days for each reponse
            runningTotalOil[index] = runningTotalOil[index] + oilVolumeClean
            runningTotalGas[index] = runningTotalGas[index] + gasVolumeClean
            numberOfDaysBattery[index] = numberOfDaysBattery[index] + 1

            if newDay == False:
                gotDayData[index] = True

            # 14 day running average code
            fourteenDayOilData[index][batteryIdCounterFourteen[index]
                                      ] = oilVolumeClean
            fourteenDayGasData[index][batteryIdCounterFourteen[index]
                                      ] = gasVolumeClean
            lastFourteenDayTotalOil = sum(fourteenDayOilData[index]) / (14)
            lastFourteenDayTotalGas = sum(fourteenDayGasData[index]) / (14)
            if batteryIdCounterFourteen[index] < 13:
                batteryIdCounterFourteen[index] = batteryIdCounterFourteen[index] + 1
            else:
                batteryIdCounterFourteen[index] = 0

            # seven day running average code
            sevenDayOilData[index][batteryIdCounterSeven[index]
                                   ] = oilVolumeClean
            sevenDayGasData[index][batteryIdCounterSeven[index]
                                   ] = gasVolumeClean
            lastSevenDayTotalOil = sum(sevenDayOilData[index]) / (7)
            lastSevenDayTotalGas = sum(sevenDayGasData[index]) / (7)

            if batteryIdCounterSeven[index] < 6:
                batteryIdCounterSeven[index] = batteryIdCounterSeven[index] + 1
            else:
                batteryIdCounterSeven[index] = 0
        else:  # if batteryId is not in list, then add to list and roll up
            wellIdList.append(batteryId)
            index = wellIdList.index(batteryId)
            runningTotalOil.insert(index, oilVolumeClean)
            runningTotalGas.insert(index, gasVolumeClean)
            numberOfDaysBattery.insert(index, 1)
            wellNameList.insert(index, batteryName)
            lastSevenDayTotalOil = oilVolumeClean
            lastFourteenDayTotalOil = oilVolumeClean
            lastSevenDayTotalGas = gasVolumeClean
            lastFourteenDayTotalGas = gasVolumeClean
            if newDay == False or currentRow == (numEntries - 1):
                gotDayData[index] = True

        # Summing today, yesterday and last week oil gas and water
        if year == todayYear and month == todayMonth and day == todayDay:
            totalOilVolume = totalOilVolume + oilVolumeClean
            totalGasVolume = totalGasVolume + gasVolumeClean
            totalWaterVolume = totalWaterVolume + waterVolumeClean

        rollingFourteenDayPerWellOil[index] = lastFourteenDayTotalOil
        rollingFourteenDayPerWellGas[index] = lastFourteenDayTotalGas

        # Checks to see if the parsed day is equal to two days ago - adds oil/gas volume to counter
        if year == twoDayYear and month == twoDayMonth and day == twoDayDay:
            twoDayOilVolume = twoDayOilVolume + oilVolumeClean
            twoDayGasVolume = twoDayGasVolume + gasVolumeClean

        # Checks to see if the parsed day is equal to yesterday days ago - adds oil/gas volume to counter
        if year == yesYear and month == yesMonth and day == yesDay:
            yesTotalOilVolume = yesTotalOilVolume + oilVolumeClean
            yesTotalGasVolume = yesTotalGasVolume + gasVolumeClean

            # for one day ago - checks if batteryId is in wellIdList
            if batteryId in wellIdList:  # if yes, does data exisit and logs correct boolean
                if oilDataExist == True:
                    wellOilVolumeOneDayAgo[index] = oilVolumeClean
                else:
                    wellOilVolumeOneDayAgo[index] = "No Data Reported"
                if gasDataExist == True:
                    wellGasVolumeOneDayAgo[index] = gasVolumeClean
                else:
                    wellGasVolumeOneDayAgo[index] = "No Data Reported"

        # Checks to see if the parsed day is equal to three days ago - adds oil/gas volume to counter
        if year == threeDayYear and month == threeDayMonth and day == threeDayDay:
            threeDayOilVolume = threeDayOilVolume + oilVolumeClean
            threeDayGasVolume = threeDayGasVolume + gasVolumeClean

        # Checks to see if the parsed day is equal to last week - adds oil/gas volume to counter
        if year == lastWeekYear and month == lastWeekMonth and day == lastWeekDay:
            lastWeekTotalOilVolume = lastWeekTotalOilVolume + oilVolumeClean
            lastWeekTotalGasVolume = lastWeekTotalGasVolume + gasVolumeClean

        # Splits battery name up
        splitString = re.split("-|–", batteryName)
        # sets client name to client name from ETX/STX and GCT
        clientName = splitString[0]
        # if field name exisits - add the batteryName
        if len(splitString) >= 3:
            batteryNameBetter = splitString[1]
            for i in range(2, len(splitString)):
                batteryNameBetter = batteryNameBetter + "-" + splitString[i]
        else:
            batteryNameBetter = splitString[1]

        index = listOfBatteryIds.index(batteryId)
        goodBatteryNameWrite = goodBatteryNames[index]

        # cleaning up the strings for outputing
        batteryNameBetter = batteryNameBetter.replace(" ", "")
        clientName = clientName.replace(" ", "")
        dateString = str(month) + "/" + str(day) + "/" + str(year)

        # grabs current time
        currentTime = dt.date(year, month, day)

        # switches clinet names to more easily viewable items
        if currentTime >= referenceTime15Day:
            if clientName == "CWS":
                clientName = "South Texas"
            elif clientName == "Peak":
                clientName = "East Texas"
            elif clientName == "Otex":
                clientName = "Gulf Coast"
            elif clientName == "Midcon":
                clientName = "Midcon"
            elif clientName == "Wellman":
                clientName = "Permian Basin"
            elif clientName == "Wellington":
                clientName = "Colorado"
            elif clientName == "Scurry":
                clientName = "Scurry"
            elif clientName == "Wyoming":
                clientName == "Wyoming"

            # creates a newRow
            newRow = [
                dateString,
                clientName,
                goodBatteryNameWrite,
                str(oilVolumeClean),
                str(gasVolumeClean),
                str(waterVolumeClean),
                str(lastSevenDayTotalOil),
                str(lastFourteenDayTotalOil),
                str(lastSevenDayTotalGas),
                str(lastFourteenDayTotalGas),
            ]
            # STARTING HERE WITH IF STATEMENT
            if (startingIndex + j) > (initalSizeOfTotalAssetProduction - 1):
                totalAssetProduction.loc[startingIndex + j] = newRow
            else:
                totalAssetProduction.iloc[startingIndex + j] = newRow

            j = j + 1

        priorDay = day

    prettyNameWellOilSoldList = []

    for i in range(0, len(wellIdOilSoldList)):
        index = listOfBatteryIds.index(wellIdOilSoldList[i])
        goodBatteryNameWrite = goodBatteryNames[index]
        prettyNameWellOilSoldList.insert(i, goodBatteryNameWrite)

    # creates an running average
    for i in range(0, len(wellIdList)):
        avgOilList.insert(i, runningTotalOil[i] / numberOfDaysBattery[i])
        avgGasList.insert(i, runningTotalGas[i] / numberOfDaysBattery[i])

    yesWellReportFileName = workingDataDirectory + r"\yesterdayWellReport.csv"
    fpReported = open(yesWellReportFileName, "w")
    headerString = "Battery ID,Battery Name,Oil Production,14-day Oil Average,Gas Production,14-day Gas Average, Pumper Name\n"
    fpReported.write(headerString)

    for i in range(0, len(wellIdList)):
        if i < len(rollingFourteenDayPerWellOil) and i < len(rollingFourteenDayPerWellGas):
            index = listOfBatteryIds.index(wellIdList[i])
            outputString = (
                str(wellIdList[i])
                + ","
                + wellNameList[i]
                + ","
                + str(wellOilVolumeOneDayAgo[i])
                + ","
                + str(rollingFourteenDayPerWellOil[i])
                + ","
                + str(wellGasVolumeOneDayAgo[i])
                + ","
                + str(rollingFourteenDayPerWellGas[i])
                + ","
                + str(pumperNames[index])
                + "\n"
            )
        else:
            outputString = (
                str(wellIdList[i])
                + ","
                + wellNameList[i]
                + ","
                + "-"
                + ","
                + str(avgOilList[i])
                + ","
                + "-"
                + ","
                + str(avgGasList[i])
                + ","
                + str(pumperNames.index(wellIdList[i]))
                + "\n"
            )

        fpReported.write(outputString)

    fpReported.close()

    # Determines whether or not a well is not reported
    notReportedListOil = []
    notReportedListGas = []
    pumperNotReportedList = []
    numberOfNotReported = 0

    for i in range(0, len(wellIdList)):
        if wellOilVolumeOneDayAgo[i] == "No Data Reported" and rollingFourteenDayPerWellOil[i] > 0:
            index = listOfBatteryIds.index(wellIdList[i])
            goodBatteryNameWrite = goodBatteryNames[index]
            notReportedListOil.append(goodBatteryNameWrite)
            pumperName = pumperNames[index]
            numberOfNotReported = numberOfNotReported + 1
            if pumperName not in pumperNotReportedList:
                pumperNotReportedList.append(pumperName)
        if wellGasVolumeOneDayAgo[i] == "No Data Reported" and rollingFourteenDayPerWellGas[i] > 0:
            index = listOfBatteryIds.index(wellIdList[i])
            goodBatteryNameWrite = goodBatteryNames[index]
            notReportedListGas.append(goodBatteryNameWrite)
            pumperName = pumperNames[index]
            if pumperName not in pumperNotReportedList:
                pumperNotReportedList.append(pumperName)

    numberOfBatteries = len(wellIdList)
    percentNotReported = (len(notReportedListOil) +
                          len(notReportedListGas)) / numberOfBatteries

    # Oil and gas daily change numbers
    oilChangeDaily = round((twoDayOilVolume - threeDayOilVolume), 2)
    gasChangeDaily = round((twoDayGasVolume - threeDayGasVolume), 2)
    oilSevenDayPercent = round(
        (twoDayOilVolume - lastWeekTotalOilVolume) / lastWeekTotalOilVolume, 1
    )
    gasSevenDayPercent = round(
        (twoDayGasVolume - lastWeekTotalGasVolume) / lastWeekTotalGasVolume, 1
    )

    if oilChangeDaily > 0:
        increaseDecreaseOil = "Increase"
    else:
        increaseDecreaseOil = "Decline"

    if gasChangeDaily > 0:
        increaseDecreaseGas = "Increase"
    else:
        increaseDecreaseGas = "Decline"

    totalAssetProduction.to_csv(
        fileNameAssetProduction,
        index=False,
    )

    # Opens Oil Change File for daily specific percent change calculations
    oilGasCustomNumbersFp = open(
        r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\oilgascustomnumbers.csv",
        "w",
    )

    headerString = "Daily Oil Change,Daily Gas Change, 7-day Oil Percent Change, 7-day Gas Percent Change, Two Day Ago Oil Volume, Two Day Ago Gas Volume, Increase/Decrease Oil, Increase/Decrease Gas, Percent Not Reported\n"

    oilGasCustomNumbersFp.write(headerString)

    outputString = (
        str(oilChangeDaily)
        + ","
        + str(gasChangeDaily)
        + ","
        + str(oilSevenDayPercent)
        + ","
        + str(gasSevenDayPercent)
        + ","
        + str(twoDayOilVolume)
        + ","
        + str(twoDayGasVolume)
        + ","
        + increaseDecreaseOil
        + ","
        + increaseDecreaseGas
        + ","
        + str(percentNotReported)
    )

    oilGasCustomNumbersFp.write(outputString)
    oilGasCustomNumbersFp.close()

    # Rounds the volumne oil sold list to two decimal places
    wellVolumeOilSoldListRound = [round(num, 2)
                                  for num in wellVolumeOilSoldList]

    # print out the volumes for data check while model is running
    print("Yesterday Oil Volume: " + str(yesTotalOilVolume))
    print("Yesterday Gas Volume: " + str(yesTotalGasVolume))
    print("Oil Sales Volume Last 30 days: " + str(thrityDayOilSalesVolume))
    print("Oil Sales Volume This Month: " + str(monthlyOilSales))

    print("Finish Rolling Up Production - Bad Pumper List Ready to Send")

    return pumperNotReportedList, totalAssetProduction


'''
GET COMMENTS FROM GREASEBOOK and returns a clean JSON file ready to loading into ComboCurve

'''


def getComments(workingDataDirectory, greasebookApi):

    workingDir = workingDataDirectory
    fileNameMasterAllocationList = workingDir + \
        r"\master\masterWellAllocation.xlsx"

    # set some date variables we will need later
    dateToday = dt.datetime.today()
    todayYear = dateToday.strftime("%Y")
    todayMonth = dateToday.strftime("%m")
    todayDay = dateToday.strftime("%d")
    dateYes = dateToday - timedelta(days=1)

    masterAllocationList = pd.read_excel(
        fileNameMasterAllocationList
    )

    productionInterval = "&start=2023-01-01&end="

    # Master API call to Greasebooks
    url = (
        "https://integration.greasebook.com/api/v1/comments/read?apiKey="
        + greasebookApi
        + productionInterval
        + todayYear
        + "-"
        + todayMonth
        + "-"
        + todayDay
        + "&pageSize=250"
    )

    # make the API call
    response = requests.request(
        "GET",
        url,
    )

    responseCode = response.status_code  # sets response code to the current state

    # parse as json string
    results = response.json()

    # checks to see if the GB API call was successful
    if responseCode == 200:
        print("Status Code is 200")
        print(str(len(results)) + " entries read")
    else:
        print("The Status Code: " + str(response.status_code))

    # gets all API's from the master allocation list
    apiList = masterAllocationList["API"].tolist()
    listOfBatteryIds = masterAllocationList["Id in Greasebooks"].tolist()

    headerCombocurve = ["Date", "API", "Comment", "Data Source"]

    # create a dataframe to hold the results
    totalCommentComboCurve = pd.DataFrame(columns=headerCombocurve)

    for currentRow in range(0, len(results)):
        row = results[currentRow]
        keys = list(row.items())

        message = ""
        batteryName = ""
        batteryId = 0
        dateOfComment = ""

        for idx, key in enumerate(keys):
            if key[0] == "message":
                message = row["message"]
            elif key[0] == "batteryId":
                batteryId = row["batteryId"]
            elif key[0] == "dateTime":
                dateOfComment = row["dateTime"]

        # spliting date correctly
        splitDate = re.split("T", dateOfComment)
        splitDate2 = re.split("-", splitDate[0])
        year = int(splitDate2[0])
        month = int(splitDate2[1])
        day = int(splitDate2[2])

        dateString = str(month) + "/" + str(day) + "/" + str(year)
        dateStringComboCurve = datetime.strptime(dateString, "%m/%d/%Y")

        batteryIdIndex = listOfBatteryIds.index(batteryId)
        api = apiList[batteryIdIndex]

        apiIdLength = len(str(api))

        if apiIdLength != 14:
            api = "0" + str(api)

        row = [dateStringComboCurve, str(api), str(message), "di"]

        totalCommentComboCurve.loc[len(totalCommentComboCurve)] = row

    totalCommentComboCurve = totalCommentComboCurve.astype({"Date": "string"})
    totalCommentComboCurve.rename(columns={
        "Date": "date", "API": "chosenID", "Comment": "operationalTag", "Data Source": "dataSource"}, inplace=True)

    totalCommentComboCurveJson = totalCommentComboCurve.to_json(
        orient="records")
    cleanTotalCommentComboCurveJson = json.loads(totalCommentComboCurveJson)

    return cleanTotalCommentComboCurveJson


# This function will allocated production by both SubAccount ID (accounting purposes) and API14 (engineering purposes)
def allocateWells(pullProd, days, workingDataDirectory, greasebookApi):

    print("Begin Allocation of Production")

    # 30 Day Or Full? If False - only looking at last 30 days and appending.
    fullProductionPull = pullProd
    numberOfDaysToPull = days

    fileNameAccounting = (workingDataDirectory +
                          r"\accountingAllocatedProduction.csv")
    fileNameComboCurve = (workingDataDirectory +
                          r"\comboCurveAllocatedProduction.csv")
    fileNameForecast = (workingDataDirectory + r"\forecastWells.csv")
    fileNameMasterAllocationList = (
        workingDataDirectory + r"\master\masterWellAllocation.xlsx")
    load_dotenv()  # load ENV

    # adding the Master Battery List for Analysis
    masterAllocationList = pd.read_excel(fileNameMasterAllocationList)
    forecastedAllocatedProduction = pd.read_csv(fileNameForecast)

    # set some date variables we will need later
    dateToday = dt.datetime.today()
    todayYear = dateToday.strftime("%Y")
    todayMonth = dateToday.strftime("%m")
    todayDay = dateToday.strftime("%d")
    dateYes = dateToday - timedelta(days=1)
    yesDayString = dateYes.strftime("%d")

    # Set production interval based on boolen
    if fullProductionPull == True:
        productionInterval = "&start=2021-04-01&end="
    else:
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

    if responseCode == 200:
        print("Sucessful Greasebook API Call")
        print(str(numEntries) + " total number of rows")
    else:
        print("The Status Code: " + str(response.status_code))

    # checks if we need to pull all the data or just last specificed
    if fullProductionPull == False:
        # Opening Master CSV for total allocation production by Subaccount
        totalAccountingAllocatedProduction = pd.read_csv(fileNameAccounting)
        totalComboCurveAllocatedProduction = pd.read_csv(fileNameComboCurve)
    else:
        headerList = [
            "Date",
            "Client",
            "Subaccount",
            "Well Accounting Name",
            "Oil Volume",
            "Gas Volume",
            "Water Volume",
            "Oil Sold Volume",
            "Oil Forecast",
            "Gas Forecast",
            "Water Forecast"
        ]
        totalAccountingAllocatedProduction = pd.DataFrame(columns=headerList)

        headerCombocurve = [
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
        totalComboCurveAllocatedProduction = pd.DataFrame(
            columns=headerCombocurve)

    # a bunch of variables the below loop needs
    wellIdList = []
    wellNameList = []
    wellIdOilSoldList = []
    wellVolumeOilSoldList = []
    gotDayData = np.full([200], False)
    totalOilVolume = 0
    totalGasVolume = 0
    totalWaterVolume = 0
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

    # Convert all dates to str for comparison rollup
    todayYear = int(dateToday.strftime("%Y"))
    todayMonth = int(dateToday.strftime("%m"))
    todayDay = int(dateToday.strftime("%d"))

    # if not pulling all of production - then just get the list of dates to parse
    if fullProductionPull == False:
        # gets list of dates
        listOfDates = totalComboCurveAllocatedProduction["Date"].to_list()
        # finds out what date is last
        lastRow = results[0]
        # lastRow = totalComboCurveAllocatedProduction.iloc[len(
        #    totalComboCurveAllocatedProduction) - 1]
        dateOfLastRow = lastRow["date"]
        splitDate = re.split("T", str(dateOfLastRow))  # splits date correct
        splitDate2 = re.split("-", str(splitDate[0]))  # splits date correct
        day = int(splitDate2[2])  # gets the correct day
        month = int(splitDate2[1])  # gets the correct month
        year = int(splitDate2[0])  # gets the correct
        referenceTimeDay = dt.date(year, month, day) - \
            timedelta(days=numberOfDaysToPull)  # creates a reference time
        dateOfInterest = referenceTimeDay.strftime(
            "%#m/%#d/%Y")  # converts to string
        startingIndex = listOfDates.index(
            dateOfInterest)  # create index surrounding
    else:
        startingIndex = 0

    # Gets list of Battery id's that are clean for printing
    listOfBatteryIds = masterAllocationList["Id in Greasebooks"].tolist()
    wellNameAccountingList = masterAllocationList["Name in Accounting"].tolist(
    )
    accountingIdList = masterAllocationList["Subaccount"].tolist()
    apiList = masterAllocationList["API"].tolist()
    allocationOilList = masterAllocationList["Allocation Ratio Oil"].tolist()
    allocationGasList = masterAllocationList["Allocation Ratio Gas"].tolist()
    stateList = masterAllocationList["State"].tolist()

    kComboCurve = 0  # variable for loop
    kAccounting = 0  # variable for loop

    # gets length of totalAllocatedProduction
    initalSizeOfTotalAllocatedProduction = len(
        totalAccountingAllocatedProduction)

    lastDate = ""
    wellsWithoutForecastCounter = 0
    wellsWithoutForecast = []

    # MASTER loop that goes through each of the items in the response
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
                if batteryId == 23012:
                    junk = 1
                if batteryId == 23011:
                    junnk = 1
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
        subAccountIdIndex = []  # empty list for index of subaccount id INDEX - used for matching

        wellAccountingName = []

        # gets all the indexs that match the battery id (sometimes 1, sometimes more)
        batteryIndexId = [m for m, x in enumerate(
            listOfBatteryIds) if x == batteryId]
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
                wellAccountingName.append(
                    wellNameAccountingList[batteryIndexId[t]])

        dateString = str(month) + "/" + str(day) + "/" + str(year)

        # clears the counters if the date changes
        if lastDate != dateString:
            welopCounter = 0
            welopGasVolume = 0
            welopOilVolume = 0
            welopWaterVolume = 0
            welopOilSalesVolume = 0
        # adamsRanchCounter = 0
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

        currentState = ""

        apiNumber = apiList[batteryIndexId[0]]
        indexList = [index for index, value in enumerate(
            forecastedAllocatedProduction["API 14"].to_list()) if value == apiNumber]

        forecastedDateList = [forecastedAllocatedProduction["Date"][index]
                              for index in indexList]

        if dateString in forecastedDateList:
            forecastedIndex = forecastedDateList.index(dateString)
            oilVolumeForecast = forecastedAllocatedProduction["Oil"][indexList[forecastedIndex]]
            gasVolumeForecast = forecastedAllocatedProduction["Gas"][indexList[forecastedIndex]]
            waterVolumeForecast = forecastedAllocatedProduction["Water"][indexList[forecastedIndex]]
        else:
            oilVolumeForecast = 0
            gasVolumeForecast = 0
            waterVolumeForecast = 0

        # CORE LOGIC FOR WELOP
        if len(batteryIndexId) == 1:
            if batteryId != 23012:
                newRow = [dateString, clientName, str(subAccountId), str(wellAccountingName), str(oilVolumeClean), str(
                    gasVolumeClean), str(waterVolumeClean), str(oilSalesDataClean), str(oilVolumeForecast), str(gasVolumeForecast), str(waterVolumeForecast)]
                newRowComboCurve = [dateString, clientName, str(apiList[batteryIndexId[0]]), str(wellAccountingName), str(oilVolumeClean), str(gasVolumeClean), str(
                    waterVolumeClean), str(oilSalesDataClean), str(oilVolumeForecast), str(gasVolumeForecast), str(waterVolumeForecast), "di", str(stateList[batteryIndexId[0]])]

                totalAccountingAllocatedProduction.loc[startingIndex +
                                                       kAccounting] = newRow  # sets new row to accounting
                totalComboCurveAllocatedProduction.loc[startingIndex +
                                                       kComboCurve] = newRowComboCurve  # sets new row to combo curve
                kComboCurve = kComboCurve + 1  # counter for combo curve
                kAccounting = kAccounting + 1  # counter for accounting

        elif len(batteryIndexId) > 1:
            for j in range(len(batteryIndexId)):
                wellOilVolume = oilVolumeClean * allocationRatioOil[j]/100
                wellGasVolume = gasVolumeClean * allocationRatioGas[j]/100
                wellWaterVolume = waterVolumeClean * allocationRatioOil[j]/100
                wellOilSalesVolume = oilSalesDataClean * \
                    allocationRatioOil[j]/100

                if batteryId != 25381 and batteryId != 25382:  # COLORADO
                    newRow = [dateString, clientName, str(apiList[batteryIndexId[j]]), str(wellAccountingName[j]), str(wellOilVolume), str(
                        wellGasVolume), str(wellWaterVolume), str(wellOilSalesVolume), str(oilVolumeForecast), str(gasVolumeForecast), str(waterVolumeForecast), "di", str(stateList[batteryIndexId[0]])]
                    junk = 0
                else:
                    newRow = [dateString, clientName, "0" + str(apiList[batteryIndexId[j]]), str(wellAccountingName[j]), str(wellOilVolume), str(
                        wellGasVolume), str(wellWaterVolume), str(wellOilSalesVolume), str(oilVolumeForecast), str(gasVolumeForecast), str(waterVolumeForecast), "di", str(stateList[batteryIndexId[0]])]

                totalComboCurveAllocatedProduction.loc[startingIndex +
                                                       kComboCurve] = newRow
                kComboCurve = kComboCurve + 1

                if batteryId != 25381 and batteryId != 25382 and batteryId != 23012 and batteryId != 23011:
                    newRow = [dateString, clientName, str(subAccountId[j]), str(wellAccountingName[j]), str(wellOilVolume), str(
                        wellGasVolume), str(wellWaterVolume), str(wellOilSalesVolume), str(oilVolumeForecast), str(gasVolumeForecast), str(waterVolumeForecast)]
                    totalAccountingAllocatedProduction.loc[startingIndex +
                                                           kAccounting] = newRow
                    kAccounting = kAccounting + 1

            # OUTSIDE OF LOOP
            if batteryId == 25381 or batteryId == 25382:
                welopOilVolume = welopOilVolume + oilVolumeClean
                welopGasVolume = welopGasVolume + gasVolumeClean
                welopWaterVolume = welopWaterVolume + waterVolumeClean
                welopOilSalesVolume = welopOilSalesVolume + oilSalesDataClean
                welopCounter = welopCounter + 1

            if welopCounter == 2:
                newRow = [dateString, clientName, str(subAccountId[0]), str(subAccountId[0]), str(welopOilVolume), str(
                    welopGasVolume), str(welopWaterVolume), str(welopOilSalesVolume), str(oilVolumeForecast), str(gasVolumeForecast), str(waterVolumeForecast)]
                totalAccountingAllocatedProduction.loc[startingIndex +
                                                       kAccounting] = newRow
                kAccounting = kAccounting + 1
                welopCounter = 0

        lastDate = dateString  # updates the date for the next iteration

    # Output final dataframes to csv and json
    totalAccountingAllocatedProduction.to_csv(fileNameAccounting, index=False)

    print("Completed Allocation Process")

    return totalComboCurveAllocatedProduction


# This function is used to send Operations an email of the pumpers who have not gotten their data in for the day


def sendPumperEmail(pumperNotReportedList, workingDataDirectory):

    dateToday = dt.datetime.today()
    dateYes = dateToday - timedelta(days=1)

    # create today's string
    yesterdayDateString = dateYes.strftime("%m/%d/%Y")

    # CREATE ATTACHMENT FILE
    oilGasReportedFileName = workingDataDirectory + r"\yesterdayWellReport.csv"

    # SEND EMAIL FUNCTION
    def send_email(emailRecipient, emailRecipientName, emailSubject, emailMessage, attachmentOne):
        counter = 0
        email_sender = os.getenv("USERNAME_KING")
        msg = MIMEMultipart()
        msg["From"] = email_sender
        msg["To"] = emailRecipient
        msg["Subject"] = emailSubject
        msg.attach(MIMEText(emailMessage, "plain"))

        # OPENS EACH ATTACHMENTS
        with open(attachmentOne, "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            partTwo = MIMEBase("application", "octet-stream")
            partTwo.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(partTwo)

        partTwo.add_header(
            "Content-Disposition",
            f"attachment; filename= yesterdayWellReport.csv",
        )

        # ATTACHES EACH FILE TO EMAIL

        msg.attach(partTwo)

        text = msg.as_string()

        try:
            server = smtplib.SMTP("smtp.office365.com", 587)
            server.ehlo()
            server.starttls()
            server.login(os.getenv("USERNAME_KING"),
                         os.getenv("PASSWORD_KING"))
            server.sendmail(email_sender, emailRecipient, text)
            print("Bad Pumper Email sent successfully to " +
                  emailRecipientName + "")
            server.quit()
            counter = counter + 1
        except:
            print("SMPT server connection error")

        return True, counter

    # Body of the email
    message = ""
    message = message + "Pumpers To Check In With:" + \
        "\n" + "---------" + "\n"

    for i in range(0, len(pumperNotReportedList)):
        message = message + pumperNotReportedList[i] + "\n"

    # email subject
    subject = "Daily Pumper Not Reported List - " + yesterdayDateString

    # Potenital users to send to
    michaelTanner = os.getenv("MICHAEL_TANNER_EMAIL")
    michaelTannerName = os.getenv("MICHAEL_TANNER_NAME")
    gabeTatman = os.getenv("GABE_TATMAN_EMAIL")
    gabeTatmanName = os.getenv("GABE_TATMAN_NAME")
    jayYoung = os.getenv("JAY_YOUNG")
    rexGifford = os.getenv("REX_GIFFORD")
    chandlerKnox = os.getenv("CHANDLER_KNOX")
    paulGerome = os.getenv("PAUL_GEROME")
    peterSnell = os.getenv("PETER_SNELL")
    garretStacey = os.getenv("GARRET_STACEY")
    grahamPatterson = os.getenv("GRAHAM_PATTERSON")
    wesMinshall = os.getenv("WES_MINSHALL")

    # LIST TO SEND TO
    counter = send_email(
        emailRecipient=michaelTanner,
        emailRecipientName=michaelTannerName,
        emailSubject=subject,
        emailMessage=message,
        attachmentOne=oilGasReportedFileName
    )

    counter = send_email(
        emailRecipient=gabeTatman,
        emailRecipientName=gabeTatmanName,
        emailSubject=subject,
        emailMessage=message,
        attachmentOne=oilGasReportedFileName
    )

    print("Completed Sending Pumper Not Reported List to " +
          str(counter[1]) + " people")
