# Import packages needed
from ast import keyword
from http import client
from posixpath import split
from time import strftime
from black import out
import requests
import os
from datetime import date, datetime, timedelta
import datetime as dt
import glob
import re
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from openpyxl import Workbook


def dailyCost(name):

    # Set Well Name To Whatever well is needed:
    nameOfWell = name

    # Load in all files needed
    pathOfAfe = r".\kingoperating\data\afe" + "\\" + nameOfWell
    plannedCostFile = pathOfAfe + "\\" + nameOfWell + "planned.xlsx"
    plannedCostDepth = pd.read_excel(plannedCostFile)
    budgetRawString = pathOfAfe + "\\" + nameOfWell + "AfeOg.xlsx"
    actualSpendString = pathOfAfe + "\\" + nameOfWell + "ActualSpend.xlsx"
    masterMatchFile = pd.read_excel(
        r".\kingoperating\data\afe\welldriveWolfepakMatch.xlsx")
    pathOfMasterFile = pathOfAfe + "\\" + "fullreport.xlsx"
    masterAfe = pd.read_excel(pathOfMasterFile)
    actualWellCostWolfepak = pd.read_excel(actualSpendString)
    budgetRawFile = pd.read_excel(budgetRawString)
    drillingDayDescriptionPath = "C:\\Users\\mtanner\\OneDrive - King Operating\\Drilling - KOC\\daysdepthdescription.xlsx"
    descriptionFile = pd.read_excel(
        drillingDayDescriptionPath, sheet_name=nameOfWell)

    # Create all clean export files needed and write the header strings
    dailyItemCostFileName = pathOfAfe + "\\" + nameOfWell + "dailyItemCost.csv"
    dailyItemCostFp = open(dailyItemCostFileName, "w")
    daysVsDepthFileName = pathOfAfe + "\\" + nameOfWell + "daysvsdepth.csv"
    daysVsDepthFp = open(daysVsDepthFileName, "w")
    headerString = "Date, Account Number, Depth, Daily Cost Estimate, Description\n"
    dailyItemCostFp.write(headerString)
    headerString = "Date, Days, Hours, Planned Depth, Planned Cost, Daily, Daily Cost Estimated, Actual Depth, Cumulative Cost,Daily Description\n"
    daysVsDepthFp.write(headerString)

    masterAfe = masterAfe.fillna(0)  # fill all emptys with 0

    wolfepakActualDesc = masterMatchFile["Description WolfePak"].tolist()
    welldriveBudgetAccounts = masterMatchFile["Code WellDrive"].tolist()
    wolfepakActualAccounts = masterMatchFile["Code WolfePak"].tolist()
    wellEzAccounts = masterMatchFile["Code Wellez"].tolist()

    costItemListClean = []
    totalCostAllFile = []
    totalDateAllFile = []
    totalDepthAllFile = []
    totalCumulativeCost = []

    descriptionDateList = descriptionFile["Date"].tolist()

    timeStampList = masterAfe["textbox51"].tolist()
    timeStampCleanList = list(set(timeStampList))

    timeStampCleanList = [str(i) for i in timeStampCleanList]

    timeStampCleanList.sort(key=lambda date: datetime.strptime(
        date, '%Y-%m-%d %H:%M:%S'))

    dateCleanList = []
    daysList = []
    days = 0
    totalDailyCost = 0
    cumulativeCost = 0
    foundStartDate = 0

    for i in range(0, len(timeStampCleanList)):
        dt = datetime.strptime(timeStampCleanList[i], '%Y-%m-%d %H:%M:%S')
        dateCleanList.append(dt.strftime("%m/%d/%Y"))
        daysList.append(days)
        days = days + 1

    measuredDepthList = masterAfe["Textbox8.1"].tolist()
    measuredDepthListClean = list(set(measuredDepthList))

    mergedDaysDateList = list(zip(daysList, dateCleanList))

    for i in range(0, len(masterAfe)):
        row = masterAfe.iloc[i]
        date = row[0]
        if i == 0:
            lastDate = date
        dateClean = date.strftime("%m/%d/%Y")
        cost = row[115]
        description = row[113]
        if description == 0:
            description = ""
        cleanDescription = description[5:]
        cleanDescription = cleanDescription.replace(",", "")
        measuredDepth = row[70]
        activity = row[30]
        account = row[114]
        accountClean = int(abs(account))
        if accountClean > 0 and cost != 0:
            accountIndex = wellEzAccounts.index(accountClean)
            wolfepakAccount = wolfepakActualAccounts[accountIndex]
            string = (
                dateClean
                + ","
                + str(wolfepakAccount)
                + ","
                + str(measuredDepth)
                + ","
                + str(cost)
                + ","
                + str(cleanDescription)
                + "\n"
            )

            dailyItemCostFp.write(string)

        if foundStartDate == 0 and measuredDepth > 0:
            foundStartDate = 1
            day = 0

        if date == lastDate:
            totalDailyCost = totalDailyCost + cost
        else:
            cumulativeCost = cumulativeCost + totalDailyCost
            if foundStartDate == 1 and activity == "Drilling":
                row = plannedCostDepth.iloc[day]
                lastDateClean = lastDate.strftime("%m/%d/%Y")
                if lastDate in descriptionDateList:
                    index = descriptionDateList.index(lastDate)
                    description = descriptionFile["Description"].iloc[index]
                else:
                    description = ""
                outputString = (
                    lastDateClean
                    + ","
                    + str(day)
                    + ","
                    + str(row["HOURS"])
                    + ","
                    + str(row["PLAN DEPTH"])
                    + ","
                    + str(row["PLAN COST"] * -1)
                    + ","
                    + str(row["DAILY"] * -1)
                    + ","
                    + str(totalDailyCost * -1)
                    + ","
                    + str(lastMeasuredDepth)
                    + ","
                    + str(cumulativeCost * -1)
                    + ","
                    + str(description)
                    + "\n"
                )
                daysVsDepthFp.write(outputString)
                day = day + 1

            totalDailyCost = cost

        lastDate = date
        lastMeasuredDepth = measuredDepth

    cumulativeCost = cumulativeCost + totalDailyCost

    # handles the exception that there is not data in the masterAfe file
    try:
        day = day
        lastDate = date
    except NameError:
        day = 0
        lastDate = dt.datetime.today()
        lastMeasuredDepth = 0

    row = plannedCostDepth.iloc[day]  # finds the next planned cost and depth
    lastDateClean = lastDate.strftime("%m/%d/%Y")
    if lastDate in descriptionDateList:
        index = descriptionDateList.index(lastDate)
        description = descriptionFile["Description"].iloc[index]
    else:
        description = ""

    outputString = (
        lastDateClean
        + ","
        + str(day)
        + ","
        + str(row["HOURS"])
        + ","
        + str(row["PLAN DEPTH"])
        + ","
        + str(row["PLAN COST"] * -1)
        + ","
        + str(row["DAILY"] * -1)
        + ","
        + str(totalDailyCost * -1)
        + ","
        + str(lastMeasuredDepth)
        + ","
        + str(cumulativeCost * -1)
        + ","
        + str(description)
        + "\n"
    )

    daysVsDepthFp.write(outputString)

    for i in range(day + 1, len(plannedCostDepth)):
        row = plannedCostDepth.iloc[i]
        outputString = (
            ""
            + ","
            + str(i)
            + ","
            + str(row["HOURS"])
            + ","
            + str(row["PLAN DEPTH"])
            + ","
            + str(row["PLAN COST"] * -1)
            + ","
            + str(row["DAILY"] * -1)
            + ","
            + ""
            + ","
            + ""
            + ","
            + ""
            + ","
            + ""
            + "\n"
        )

        daysVsDepthFp.write(outputString)

    dailyItemCostFp.close()
    daysVsDepthFp.close()

    print("Days vs Depth vs Cost and Daily Item Cost Updated")


"""
    
 AFE vs Actual Spend
 
 
 """


def variance(name):

    # Set Well Name To Whatever well is needed:
    nameOfWell = name

    # Load in all files needed
    pathOfAfe = r".\kingoperating\data\afe" + "\\" + nameOfWell
    plannedCostFile = pathOfAfe + "\\" + nameOfWell + "planned.xlsx"
    plannedCostDepth = pd.read_excel(plannedCostFile)
    budgetRawString = pathOfAfe + "\\" + nameOfWell + "AfeOg.xlsx"
    actualSpendString = pathOfAfe + "\\" + nameOfWell + "ActualSpend.xlsx"
    masterMatchFile = pd.read_excel(
        r".\kingoperating\data\afe\welldriveWolfepakMatch.xlsx")
    pathOfMasterFile = pathOfAfe + "\\" + "fullreport.xlsx"
    masterAfe = pd.read_excel(pathOfMasterFile)
    actualWellCostWolfepak = pd.read_excel(actualSpendString)
    budgetRawFile = pd.read_excel(budgetRawString)
    masterAfe = masterAfe.fillna(0)  # fill all emptys with 0

    wolfepakActualDesc = masterMatchFile["Description WolfePak"].tolist()
    welldriveBudgetAccounts = masterMatchFile["Code WellDrive"].tolist()
    wolfepakActualAccounts = masterMatchFile["Code WolfePak"].tolist()
    wellEzAccounts = masterMatchFile["Code Wellez"].tolist()

    # Begin AFE Variance

    actualAccountCodeList = actualWellCostWolfepak["Account"].tolist()
    actualCostList = actualWellCostWolfepak["Amount"].tolist()

    # create empty lists needed for loop
    outputData = []
    accountCodeInBudgetList = []

    filePointerString = pathOfAfe + "\\" + nameOfWell + "AfeActualVarience.csv"

    fp = open(filePointerString, "w")

    headerString = "Account,Account Description,AFE Budget,Actual Spend,Varience\n"
    fp.write(headerString)

    # Master loop in order to match budget to actual
    for i in range(0, len(budgetRawFile)):
        # Budget row
        rowAfeBudgetRaw = budgetRawFile.iloc[i]
        afeBudgetAccountCode = rowAfeBudgetRaw["Account"]
        afeBudgetCost = rowAfeBudgetRaw["Cost"]

        # gets index of buget account code in actual account code
        indexBudget = welldriveBudgetAccounts.index(afeBudgetAccountCode)
        # matches with actual account code
        actualAccountCode = wolfepakActualAccounts[indexBudget]
        accountActualOccurrences = [j for j, x in enumerate(
            actualAccountCodeList) if x == actualAccountCode]

        # handling multiple transactions with same account code
        if accountActualOccurrences == []:
            actualCostClean = 0
        else:
            actualCostClean = 0
            for m in range(0, len(accountActualOccurrences)):
                actualCostClean += actualCostList[accountActualOccurrences[m]]

        indexDesc = wolfepakActualAccounts.index(actualAccountCode)

        outputData.append([actualAccountCode, wolfepakActualDesc[indexDesc],
                           afeBudgetCost, actualCostClean])

        accountCodeInBudgetList.append(actualAccountCode)

    outputData.sort(key=lambda x: x[0])
    counter = 0

    trigger = True
    while trigger == True:
        account = outputData[counter][0]
        occurences = [j for j, x in enumerate(outputData) if x[0] == account]
        budgetCost = 0
        for m in range(0, len(occurences)):
            budgetCost += outputData[occurences[m]][2]

        actualCost = outputData[counter][3]
        varience = budgetCost - actualCost

        description = str(outputData[counter][1])
        betterDesc = description.replace(",", "")
        printString = (
            str(account)
            + ","
            + str(betterDesc)
            + ","
            + str(budgetCost)
            + ","
            + str(actualCost)
            + ","
            + str(varience)
            + "\n"
        )
        fp.write(printString)

        counter = occurences[len(occurences) - 1] + 1

        if counter == len(outputData):
            trigger = False

    outputDataExtra = []

    for i in range(0, len(actualWellCostWolfepak)):
        accountCodeActual = actualWellCostWolfepak.iloc[i]["Account"]
        if accountCodeActual not in accountCodeInBudgetList:
            accountActualOccurrences = [j for j, x in enumerate(
                actualAccountCodeList) if x == accountCodeActual]

            actualCostClean = 0
            for m in range(0, len(accountActualOccurrences)):
                actualCostClean += actualCostList[accountActualOccurrences[m]]

            outputDataExtra.append(
                [accountCodeActual, actualWellCostWolfepak.iloc[i]["{Account Desc}"], 0, actualCostClean])

    # this handles when there are actual lines that are not in the budget

    counter = 0
    trigger = True
    while trigger == True and outputDataExtra != []:
        account = outputDataExtra[counter][0]
        occurences = [j for j, x in enumerate(
            outputDataExtra) if x[0] == account]

        budgetCost = 0

        actualCost = outputDataExtra[counter][3]
        varience = budgetCost - actualCost

        description = str(outputDataExtra[counter][1])
        betterDesc = description.replace(",", "")
        printString = (
            str(account)
            + ","
            + str(betterDesc)
            + ","
            + str(budgetCost)
            + ","
            + str(actualCost)
            + ","
            + str(varience)
            + "\n"
        )
        fp.write(printString)

        counter = occurences[len(occurences) - 1] + 1

        if counter == len(outputDataExtra):
            trigger = False

    fp.close()

    print("Done with AFE Varience")
