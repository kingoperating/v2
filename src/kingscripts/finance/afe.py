# Import packages needed
from datetime import datetime
import pandas as pd
from kingscripts.analytics import tech
import os

"""

Getting AFE Daily Cost Data Formatted Properly

"""


def dailyCost(workingDataDirectory, name):
    # Set Well Name To Whatever well is needed:
    nameOfWell = name
    # Load in all files needed
    pathOfWorkingDir = workingDataDirectory
    pathOfAfe = pathOfWorkingDir + r"\afe" + "\\" + nameOfWell
    plannedCostFile = pathOfAfe + "\\" + nameOfWell + "planned.xlsx"
    plannedCostDepth = pd.read_excel(plannedCostFile)
    budgetRawString = pathOfAfe + "\\" + nameOfWell + "AfeOg.xlsx"
    actualSpendString = pathOfAfe + "\\" + nameOfWell + "ActualSpend.xlsx"
    masterMatchFileString = pathOfWorkingDir + \
        r"\master\welldriveWolfepakMatch.xlsx"
    masterMatchFile = pd.read_excel(masterMatchFileString)
    pathOfMasterFile = pathOfAfe + "\\" + nameOfWell + "fullreport.xlsx"
    masterAfe = pd.read_excel(pathOfMasterFile)
    actualWellCostWolfepak = pd.read_excel(actualSpendString)
    budgetRawFile = pd.read_excel(budgetRawString)
    drillingDayDescriptionPath = workingDataDirectory + \
        r"\master\daysdepthdescription.xlsx"
    descriptionFile = pd.read_excel(
        drillingDayDescriptionPath, sheet_name=nameOfWell)

    # Create all clean export files needed and write the header strings
    dailyItemCostFileName = pathOfAfe + "\\" + nameOfWell + "dailyItemCost.csv"
    dailyItemCostFp = open(dailyItemCostFileName, "w")
    daysVsDepthFileName = pathOfAfe + "\\" + nameOfWell + "daysvsdepth.csv"
    daysVsDepthFp = open(daysVsDepthFileName, "w")
    headerString = "Date, Account Number, Depth, Daily Cost Estimate, Description\n"
    dailyItemCostFp.write(headerString)
    headerString = "Date, Days, Hours, Planned Depth, Planned Cost, Daily, Daily Cost Estimated, Actual Depth, Cumulative Cost,Daily Description,Activity\n"
    daysVsDepthFp.write(headerString)

    masterAfe = masterAfe.fillna(0)  # fill all emptys with 0

    wolfepakActualDesc = masterMatchFile["Description WolfePak"].tolist()
    welldriveBudgetAccounts = masterMatchFile["Code WellDrive"].tolist()
    wolfepakActualAccounts = masterMatchFile["Code WolfePak"].tolist()
    wellEzAccounts = masterMatchFile["Code Wellez"].tolist()

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
                    + ","
                    + str(activity)
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
        lastDate = datetime.today()
        lastMeasuredDepth = 0

    row = plannedCostDepth.iloc[day]  # finds the next planned cost and depth
    lastDateClean = lastDate.strftime("%m/%d/%Y")
    if lastDate in descriptionDateList:
        index = descriptionDateList.index(lastDate)
        description = descriptionFile["Description"].iloc[index]
        description = description.replace(",", " ")
    else:
        description = ""

    if "activity" in locals():
        activity = activity
    else:
        activity = ""
        
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
        + ","
        + str(activity)
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
            + ","
            + ""
            + "\n"
        )

        daysVsDepthFp.write(outputString)

    dailyItemCostFp.close()
    daysVsDepthFp.close()

    print("Days vs Depth vs Cost and Daily Item Cost Updated for " + nameOfWell)


"""

 AFE vs Actual Spend
 
"""


def variance(workingDataDirectory, name):
    # Set Well Name To Whatever well is needed:
    nameOfWell = name
    # Load in all files needed

    pathOfWorkingDir = workingDataDirectory
    pathOfAfe = pathOfWorkingDir + r"\afe" + "\\" + nameOfWell
    budgetRawString = pathOfAfe + "\\" + nameOfWell + "AfeOg.xlsx"
    actualSpendString = pathOfAfe + "\\" + nameOfWell + "ActualSpend.xlsx"
    masterMatchFileString = pathOfWorkingDir + \
        r"\master\welldriveWolfepakMatch.xlsx"
    masterMatchFile = pd.read_excel(masterMatchFileString)
    pathOfMasterFile = pathOfAfe + "\\" + nameOfWell + "fullreport.xlsx"
    masterAfe = pd.read_excel(pathOfMasterFile)
    actualWellCostWolfepak = pd.read_excel(actualSpendString)
    budgetRawFile = pd.read_excel(budgetRawString)
    masterAfe = masterAfe.fillna(0)  # fill all emptys with 0

    wolfepakActualDesc = masterMatchFile["Description WolfePak"].tolist()
    welldriveBudgetAccounts = masterMatchFile["Code WellDrive"].tolist()
    wolfepakActualAccounts = masterMatchFile["Code WolfePak"].tolist()

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

    print("Done with AFE Varience for " + nameOfWell)

    """
    Combine AFE reporting files: AfeActualVariance, dailyItemCost, Paid and Actual
    """


"""

Combine all AFE vs Actual Spend Files for reporting purposes
 
"""


def combineAfeFiles(listOfWells, workingDataDirectory):
    # set working data directory
    pathOfWorkingDir = workingDataDirectory + r"\afe"
    pathofExport = pathOfWorkingDir + r"\mergedFiles"

    # combinedDataFrame files
    afeVarianceCombineData = pd.DataFrame()
    dailyItemCostCombineData = pd.DataFrame()
    paidCombineData = pd.DataFrame()
    spendCombineData = pd.DataFrame()
    daysVsDepthCombineData = pd.DataFrame()
    afeOgCombineData = pd.DataFrame()
    plannedCostCombineData = pd.DataFrame()
    dataFrameStoreAfeVariance = []
    dataFrameStoreDailyItemCost = []
    dataFrameStorePaid = []
    dataFrameStoreSpend = []
    dataFrameStoreDaysVsDepth = []
    dataFrameStoreAfeOg = []
    dataFrameStorePlanned = []

    # combine all AFE Variance Files
    for i in range(0, len(listOfWells)):
        nameOfWell = listOfWells[i]
        df = pd.read_csv(pathOfWorkingDir + "\\" +
                         nameOfWell + "\\" + nameOfWell + "AfeActualVarience.csv")
        df["Well Name"] = nameOfWell
        dataFrameStoreAfeVariance.append(df)

    # combine all Daily Item Cost Files
    for i in range(0, len(listOfWells)):
        nameOfWell = listOfWells[i]
        df = pd.read_csv(pathOfWorkingDir + "\\" +
                         nameOfWell + "\\" + nameOfWell + "dailyItemCost.csv")
        df["Well Name"] = nameOfWell
        dataFrameStoreDailyItemCost.append(df)

    # combine all Paid Files
    for i in range(0, len(listOfWells)):
        nameOfWell = listOfWells[i]
        df = pd.read_excel(pathOfWorkingDir + "\\" +
                           nameOfWell + "\\" + nameOfWell + "Paid.xlsx")
        df["Well Name"] = nameOfWell
        dataFrameStorePaid.append(df)

    # combine all Spend Files
    for i in range(0, len(listOfWells)):
        nameOfWell = listOfWells[i]
        df = pd.read_excel(pathOfWorkingDir + "\\" +
                           nameOfWell + "\\" + nameOfWell + "ActualSpend.xlsx")
        df["Well Name"] = nameOfWell
        dataFrameStoreSpend.append(df)

    # combine all Days vs Depth Files
    for i in range(0, len(listOfWells)):
        nameOfWell = listOfWells[i]
        path = pathOfWorkingDir + "\\" + nameOfWell + \
            "\\" + nameOfWell + "daysvsdepth.csv"
        df = pd.read_csv(path)
        df["Well Name"] = nameOfWell
        dataFrameStoreDaysVsDepth.append(df)

    # combine all AFE Og Files
    for i in range(0, len(listOfWells)):
        nameOfWell = listOfWells[i]
        df = pd.read_excel(pathOfWorkingDir + "\\" +
                           nameOfWell + "\\" + nameOfWell + "AfeOg.xlsx")
        df["Well Name"] = nameOfWell
        dataFrameStoreAfeOg.append(df)

    # combine all Planned Cost Files
    for i in range(0, len(listOfWells)):
        nameOfWell = listOfWells[i]
        df = pd.read_excel(pathOfWorkingDir + "\\" +
                           nameOfWell + "\\" + nameOfWell + "planned.xlsx")
        df["Well Name"] = nameOfWell
        dataFrameStorePlanned.append(df)

    # combine all dataframes
    afeVarianceCombineData = pd.concat(dataFrameStoreAfeVariance)
    dailyItemCostCombineData = pd.concat(dataFrameStoreDailyItemCost)
    spendCombineData = pd.concat(dataFrameStoreSpend)
    paidCombineData = pd.concat(dataFrameStorePaid)
    daysVsDepthCombineData = pd.concat(dataFrameStoreDaysVsDepth)
    #daysVsDepthCombineData = daysVsDepthCombineData.fillna(0)
    afeOgCombineData = pd.concat(dataFrameStoreAfeOg)
    plannedCostCombineData = pd.concat(dataFrameStorePlanned)

    # PUT all dataframes in SQL Server
    tech.putData(
        server=str(os.getenv("SQL_SERVER_KING_DATAWAREHOUSE")),
        database=str(os.getenv("SQL_AFE_DATABASE")),
        tableName="prod_afe_variance",
        data=afeVarianceCombineData,
    )
    tech.putData(
        server=str(os.getenv("SQL_SERVER_KING_DATAWAREHOUSE")),
        database=str(os.getenv("SQL_AFE_DATABASE")),
        tableName="prod_daily_item_cost",
        data=dailyItemCostCombineData,
    )
    tech.putData(
        server=str(os.getenv("SQL_SERVER_KING_DATAWAREHOUSE")),
        database=str(os.getenv("SQL_AFE_DATABASE")),
        tableName="prod_spend",
        data=spendCombineData,
    )
    tech.putData(
        server=str(os.getenv("SQL_SERVER_KING_DATAWAREHOUSE")),
        database=str(os.getenv("SQL_AFE_DATABASE")),
        tableName="prod_paid",
        data=paidCombineData,
    )
    tech.putData(
        server=str(os.getenv("SQL_SERVER_KING_DATAWAREHOUSE")),
        database=str(os.getenv("SQL_AFE_DATABASE")),
        tableName="prod_days_vs_depth",
        data=daysVsDepthCombineData,
    )
    tech.putData(
        server=str(os.getenv("SQL_SERVER_KING_DATAWAREHOUSE")),
        database=str(os.getenv("SQL_AFE_DATABASE")),
        tableName="prod_afe_master",
        data=afeOgCombineData,
    )
    tech.putData(
        server=str(os.getenv("SQL_SERVER_KING_DATAWAREHOUSE")),
        database=str(os.getenv("SQL_AFE_DATABASE")),
        tableName="prod_planned_cost_depth",
        data=plannedCostCombineData,
    )

    afeVarianceCombineData.to_excel(
        pathofExport + r"\afeVarianceCombineData.xlsx", index=False)
    dailyItemCostCombineData.to_excel(
        pathofExport + r"\dailyItemCostCombineData.xlsx", index=False)
    spendCombineData.to_excel(
        pathofExport + r"\spendCombineData.xlsx", index=False)
    paidCombineData.to_excel(
        pathofExport + r"\paidCombineData.xlsx", index=False)
    daysVsDepthCombineData.to_excel(
        pathofExport + r"\daysVsDepthCombineData.xlsx", index=False)
    afeOgCombineData.to_excel(
        pathofExport + r"\afeOgCombineData.xlsx", index=False)
    plannedCostCombineData.to_excel(
        pathofExport + r"\plannedCostCombineData.xlsx", index=False)
