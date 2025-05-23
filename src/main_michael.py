"""
Main Script for KOC Python Packages 3.4.0

Developer: Michael Tanner

"""
# KOC v3.3.0 Python Packages
from kingscripts.operations import greasebook, combocurve, joyn, zdscada
from kingscripts.analytics import enverus, king, tech
from kingscripts.finance import wenergy, afe

#  Needed Python Packages
from dotenv import load_dotenv
from combocurve_api_v1 import ServiceAccount
import os
import datetime as dt
from datetime import timedelta
import pandas as pd

# load .env file
load_dotenv()

'''
FIRST - MAKE SURE ALL THE ENVIRONMENT VARIABLES ARE SET IN THE .ENV FILE

SECOND - ENSURE YOUR WORKING DATA DIRECTORY IS SET TO THE CORRECT FOLDER. CURRENT THIS SCRIPT REFERENCES THE KOC DATAWAREHOUSE V1.0.0

'''
# Working Directories
workingDirectoryData = os.getenv("WORKING_DIRECTORY_DATA")
kocDatawarehouse = os.getenv("KOC_DATAWAREHOUSE")

# getting API keys
enverusApiKey = os.getenv('ENVERUS_API')
greasebookApi = os.getenv('GREASEBOOK_API_KEY')
# serviceAccount = ServiceAccount.from_file(
    # os.getenv("KING_COMBOCURVE_API_SEC_CODE_LIVE"))
comboCurveApiKey = os.getenv("KING_COMBOCURVE_API_KEY_PASS_LIVE")
joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))
## import ZDSCADA API Username / Password and Company ID
zdscadaUsername = str(os.getenv('ZDSCADA_API_USERNAME'))
zdscadaPassword = str(os.getenv('ZDSCADA_API_PASSWORD'))
zdscadaCompanyID = str(os.getenv('ZDSCADA_API_COMPANY_ID'))
function = "main"

## SQL Server Variables - for KOC Datawarehouse 3.0
kingServerExpress = str(os.getenv('SQL_SERVER'))
kingDatabaseExpress = str(os.getenv('SQL_KING_DATABASE'))
kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATAWAREHOUSE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))
kingPlanningDatabase = str(os.getenv('SQL_PLANNING_DATABASE'))
badPumperTable = "prod_bad_pumper_data"
kingPlanningDataRaw = str(os.getenv("KING_PLANNING_DATA_RAW"))
sqlMichaelTannerUsername = str(os.getenv('SQL_SERVER_MICHAEL_TANNER_USERNAME'))
sqlMichaelTannerPassword = str(os.getenv('SQL_SERVER_MICHAEL_TANNER_PASSWORD'))

## Names of KOC Employees
michaelTanner = os.getenv("MICHAEL_TANNER_EMAIL")
michaelTannerName = os.getenv("MICHAEL_TANNER_NAME")
gabeTatman = os.getenv("GABE_TATMAN_EMAIL")
gabeTatmanName = os.getenv("GABE_TATMAN_NAME")
garrettStacey = os.getenv("GARRETT_STACEY_EMAIL")
garrettStaceyName = os.getenv("GARRETT_STACEY_NAME")
nathanMyers = os.getenv("NATHAN_MYERS_EMAIL")
nathanMyersName = os.getenv("NATHAN_MYERS_NAME")

# Getting Date Variables
dateToday = dt.datetime.today()
isTuseday = dateToday.weekday()
dateLastSunday = dateToday - timedelta(days=2)
dateEightDaysAgo = dateToday - timedelta(days=8)
dateYesterday = dateToday - timedelta(days=1)
todayYear = dateToday.strftime("%Y")
todayMonth = dateToday.strftime("%m")
todayDay = dateToday.strftime("%d")

yesYear = int(dateYesterday.strftime("%Y"))
yesMonth = int(dateYesterday.strftime("%m"))
yesDay = int(dateYesterday.strftime("%d"))
yesDateString = dateYesterday.strftime("%Y-%m-%d")
todayDateString = dateToday.strftime("%Y-%m-%d")
eightDayAgoString = dateEightDaysAgo.strftime("%Y-%m-%d")

# Important Variables for scripts
nglStream = 0
noNglStream = 1
texas = "texas"
wyoming = "wyoming"
browning5181H = "42033325890000"
browningOperatorName = "BROWNING OIL"
basin = "MIDLAND"
comboCurveProjectId = "612fc3d36880c20013a885df"
comboCurveScenarioId = "64aca0abaa25aa001201b299"
comboCurveForecastId = "64a5d95390c0320012a83df9"
millerranchb501mh = "millerranchb501mh"
millerrancha502v = "millerrancha502v"
millerrancha501mh = "millerrancha501mh"
millerranchc301 = "millerranchc301"
millerranchc302mh = "millerranchc302mh"
thurman23v = "thurman23v"
chunn923v = "chunn923v"
ayres79v = "ayres79v"
porter33v = "porter33v"
wu108 = "wu108"
wu105 = "wu105"
wu99 = "wu99"
kinga199cv1h = "kinga199cv1h"
kinga199cv2h = "kinga199cv2h"
nameOfWell = "thurman23v"
irvinSisters53m1h = "irvinsisters53m1h"
pshigoda752h = "pshigoda752h"
itSqlTable = "itSpend"
daysToPull = 35
daysToLookBack = 2
listOfWells = [
    irvinSisters53m1h,
    pshigoda752h,
    thurman23v,
    chunn923v,
    ayres79v,
    porter33v,
    kinga199cv1h,
    kinga199cv2h,
    wu108,
    wu105,
    wu99,
    millerrancha501mh,
    millerrancha502v,
    millerranchb501mh,
    millerranchc301,
    millerranchc302mh
]

pathToEchoUnit2252 = str(os.getenv("ECHO_UNIT_2252"))
joynUser = str(os.getenv('JOYN_USER'))
pathToBrowning = str(os.getenv("BROWNING"))

'''
WORKING ZONE
'''

data = king.getBrowningProduction(
    pathToData=pathToBrowning, 
    wellName="Bertha 28-6 # 1H"
)

x=5

userId = joyn.getJoynUser(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    nameToFind=joynUser
)

echoUnit2252Id = joyn.getWellObjectId(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    nameOfWell="ECHO BS UNIT A 2252H"
)

print(echoUnit2252Id[1])

echoUnit2252Data = king.getConocoEchoUnit(
    pathToFolder=pathToEchoUnit2252,
    daysToLookBack=1
)

joyn.putJoynData(
    userId=userId,
    rawData=echoUnit2252Data,
    objectId=echoUnit2252Id,
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

data = pd.read_csv(r"C:\Users\Michael Tanner\Downloads\business_associates.csv")

tech.putDataReplace(
    server=kingLiveServer,
    database="test",
    tableName="business_associates",
    data=data,
    uid=sqlMichaelTannerUsername,
    password=sqlMichaelTannerPassword
)

ko200 = pd.read_excel(r"C:\Users\Michael Tanner\OneDrive - King Operating\KOC Datawarehouse\finance\WolfePak\KO200\KO200_GL.xlsx")
koand = pd.read_excel(r"C:\Users\Michael Tanner\OneDrive - King Operating\KOC Datawarehouse\finance\WolfePak\KOAND\KOAND_GL.xlsx")
kobor = pd.read_excel(r"C:\Users\Michael Tanner\OneDrive - King Operating\KOC Datawarehouse\finance\WolfePak\KOBOR\KOBOR_GL.xlsx")
koeas = pd.read_excel(r"C:\Users\Michael Tanner\OneDrive - King Operating\KOC Datawarehouse\finance\WolfePak\KOEAS\KOEAS_GL.xlsx")
kogct = pd.read_excel(r"C:\Users\Michael Tanner\OneDrive - King Operating\KOC Datawarehouse\finance\WolfePak\KOGCT\KOGCT_GL.xlsx")
koprm = pd.read_excel(r"C:\Users\Michael Tanner\OneDrive - King Operating\KOC Datawarehouse\finance\WolfePak\KOPRM\KOPRM_GL.xlsx")
kosou = pd.read_excel(r"C:\Users\Michael Tanner\OneDrive - King Operating\KOC Datawarehouse\finance\WolfePak\KOSOU\KOSOU_GL.xlsx")
kowy2 = pd.read_excel(r"C:\Users\Michael Tanner\OneDrive - King Operating\KOC Datawarehouse\finance\WolfePak\KOWY2\KOWY2_GL.xlsx")
kowym = pd.read_excel(r"C:\Users\Michael Tanner\OneDrive - King Operating\KOC Datawarehouse\finance\WolfePak\KOWYM\KOWYM_GL.xlsx")
welop = pd.read_excel(r"C:\Users\Michael Tanner\OneDrive - King Operating\KOC Datawarehouse\finance\WolfePak\WELOP\WELOP_GL.xlsx")

## add new column to each dataframe with ko200, koand, kobor, koeas, kogct, koprm, kosou, kowy2, kowym, welop
ko200["source"] = "ko200"
koand["source"] = "koand"
kobor["source"] = "kobor"
koeas["source"] = "koeas"
kogct["source"] = "kogct"
koprm["source"] = "koprm"
kosou["source"] = "kosou"
kowy2["source"] = "kowy2"
kowym["source"] = "kowym"
welop["source"] = "welop"

print("done loading all data")

data = pd.concat([ko200, koand, kobor, koeas, kogct, koprm, kosou, kowy2, kowym, welop], ignore_index=True)

print("done")

tech.putDataReplace(
    server=kingLiveServer,
    database="finance",
    data=data,
    tableName="wolfepak_gl"
)


conocoData = king.getConocoEchoUnit(
    pathToFolder=r"C:\Users\mtanner\OneDrive - King Operating\PowerAutomate\ECHO UNIT 2252",
    daysToLookBack=10
)

data = zdscada.getScadaToken(
    username=zdscadaUsername,
    password=zdscadaPassword,
    companyid=zdscadaCompanyID
)


data = combocurve.getLatestScenarioMonthly(
    projectIdKey="653fcf9ec230f38554b3f1c1",
    scenarioIdKey="653fd02e16729ef21ebd8f08",
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

# crest = combocurve.ccScenarioToCrestFpSingleWell(
#     comboCurveScenarioData=data,
#     nglYield=noNglStream,
#     gasBtuFactor=1,
#     gasShrinkFactor=0,
#     oilPricePercent=1,
#     gasPricePercent=1,
#     nglPricePercent=.35,
#     oilVariableCost=0,
#     gasVariableCost=.5,
#     nglVariableCost=0,
#     waterVariableCost=.6,
#     state=texas,
#     capex=6600000,
# )

crestPdp = combocurve.ccScenarioToCrestFpPdp(
    comboCurveScenarioData=data,
    nglYield=noNglStream,
    gasBtuFactor=1,
    gasShrinkFactor=0,
    oilPricePercent=1,
    gasPricePercent=1,
    nglPricePercent=.3,
)

crestPdp.to_csv(r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\production\crestPdp312024withCholla.csv", index=False)


path = r"C:\Users\mtanner\OneDrive - King Operating\PowerAutomate\WU108"

wu108 = king.getWorlandUnit108Production(
    pathToFolder=path
)

userId = joyn.getJoynUser(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    nameToFind="mtanner"
)

testWellObjectId = joyn.getWellObjectId(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    nameOfWell="test_well"
)

joyn.putJoynData(
    userId=userId,
    rawData=wu108,
    objectId=testWellObjectId,
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

path = r"C:\Users\mtanner\OneDrive - King Operating\PowerAutomate\Buffalo 6-8"

buffalo = king.getChollaData(
    pathToFolder=path
)

allocatedProd = tech.getData(
    server=kingLiveServer,
    database=kingProductionDatabase,
    tableName="daily_production"
)

wellHeaderData = tech.getData(
    server=kingLiveServer,
    database="wells",
    tableName="header_data"
)

combocurve.putJoynWellProductionData(
    allocatedProductionMaster=allocatedProd,
    comboCurveApi=comboCurveApiKey,
    serviceAccount=serviceAccount,
    daysToLookback=3,
    headerData=wellHeaderData
)



wellHeaderData = joyn.getWellHeaderData(
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

tech.putDataReplace(
    server=kingLiveServer,
    database="wells",
    data=wellHeaderData,
    tableName="header_data"
)

historicalAllocatedProduction = tech.getData(
    server=kingLiveServer,
    database="gabe",
    tableName="test_table"
)

print("All Historical Record Length Before Deleting: " + str(len(historicalAllocatedProduction)))

lastTwoDaysJoynData = joyn.getDailyAllocatedProductionRawWithDeleted(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    wellHeaderData=wellHeaderData,
    daysToLookBack=2
)

duplicatedIdList = joyn.compareJoynSqlDuplicates(
    joynData=lastTwoDaysJoynData,
    sqlData=historicalAllocatedProduction
)

listOfIds = duplicatedIdList["UUID"].tolist()

tech.deleteDuplicateRecords(
    server=kingLiveServer,
    database="gabe",
    tableName="test_table",
    duplicateList=listOfIds
)

lengthOfWorkingTable = tech.getData(
    server=kingLiveServer,
    database="gabe",
    tableName="test_table"
)

print("All Historical Record Length After Deleting: " + str(len(lengthOfWorkingTable)))

tech.putDataAppend(
    server=kingLiveServer,
    database="gabe",
    data=lastTwoDaysJoynData,
    tableName="test_table"
)

historicalAllocatedProduction = tech.getData(
    server=kingLiveServer,
    database="gabe",
    tableName="test_table"
)

print("All Historical Record Length Final Value: " + str(len(historicalAllocatedProduction)))


data = joyn.getDailyAllocatedProductionRaw(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    wellHeaderData=wellHeaderData,
    daysToLookBack=5
)


allocatedProd = tech.getData(
    server=kingLiveServer,
    database=kingProductionDatabase,
    tableName="allocated_production"
)

combocurve.putJoynWellProductionData(
    allocatedProductionMaster=allocatedProd,
    comboCurveApi=comboCurveApiKey,
    serviceAccount=serviceAccount,
    daysToLookback=10
)


data = combocurve.getLatestScenarioMonthly(
    projectIdKey="653fcf9ec230f38554b3f1c1",
    scenarioIdKey="653fd02e16729ef21ebd8f08",
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

# crest = combocurve.ccScenarioToCrestFpSingleWell(
#     comboCurveScenarioData=data,
#     nglYield=noNglStream,
#     gasBtuFactor=1,
#     gasShrinkFactor=0,
#     oilPricePercent=1,
#     gasPricePercent=1,
#     nglPricePercent=.35,
#     oilVariableCost=0,
#     gasVariableCost=.5,
#     nglVariableCost=0,
#     waterVariableCost=.6,
#     state=texas,
#     capex=6600000,
# )

crestPdp = combocurve.ccScenarioToCrestFpPdp(
    comboCurveScenarioData=data,
    nglYield=noNglStream,
    gasBtuFactor=1,
    gasShrinkFactor=0,
    oilPricePercent=1,
    gasPricePercent=1,
    nglPricePercent=.3,
)


crestPdp.to_csv(r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\production\crestPdpJan2024Forecast1172024.csv", index=False)



wellHeaderData = joyn.getWellHeaderData(
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

dateEnd = dt.datetime.today()

runtimeSeconds = (dateEnd - dateToday).total_seconds()

usageFunction = king.updateUsageStatsEtlRuntime(
    etlStartTime=dateToday,
    etlEndTime=dateEnd,
    function=function,
    runtime=runtimeSeconds
)

date = king.updateUsageStatsEtl(
    etlStartTime=dateToday,
    runtime=0,
    function=function,
    )



tech.putDataReplace(
    server=kingLiveServer,
    database="wells",
    data=wellHeaderData,
    tableName="header_data"
)

productTable = joyn.getProductList(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
)

tech.putDataReplace(
    server=kingLiveServer,
    database="wells",
    data=productTable,
    tableName="product_type"
)


data = tech.getData(
    server=kingLiveServer,
    database="production",
    tableName="allocated_production"
)

print(len(data))

wellData = joyn.getWellHeaderData(
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

productTable = joyn.getProductList(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
)

tech.putDataReplace(
    server=kingLiveServer,
    database="gabe",
    data=productTable,
    tableName="product_type"
)



data = joyn.getDailyAllocatedProductionRaw(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    wellHeaderData=wellData,
    daysToLookBack=2
)

masterSqlAllocatedProduction = tech.getData(
    server=kingLiveServer,
    database="gabe",
    tableName="test_table"
)

lastTwoDaysJoynData = joyn.getDailyAllocatedProductionRawWithDeleted(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    wellHeaderData=wellData,
    daysToLookBack=2
)

duplicatedIdList = joyn.compareJoynSqlDuplicates(
    joynData=lastTwoDaysJoynData,
    sqlData=masterSqlAllocatedProduction
)

tech.putDataReplace(
    server=kingLiveServer,
    database="gabe",
    data=duplicatedIdList,
    tableName="duplicates_table"
)

listOfIds = duplicatedIdList["ID"].tolist()

delete = tech.deleteDuplicateRecords(
    server=kingLiveServer,
    database="gabe",
    tableName="test_table",
    duplicateList=listOfIds
)

lengthOfWorkingTable = tech.getData(
    server=kingLiveServer,
    database="gabe",
    tableName="test_table"
)

print("All Historical Record Length After Deleting: " + str(len(lengthOfWorkingTable)))

tech.putDataAppend(
    server=kingLiveServer,
    database="gabe",
    data=lastTwoDaysJoynData,
    tableName="test_table"
)

masterSqlAllocatedProduction = tech.getData(
    server=kingLiveServer,
    database="gabe",
    tableName="test_table"
)

print("All Historical Record Length Final Value: " + str(len(masterSqlAllocatedProduction)))

x= 5


data = tech.getData(
    server=kingLiveServer,
    database="production",
    tableName="prod_daily_allocated_volume"
)

print(len(data))

tech.putDataUpdate(
    server=kingLiveServer,
    database=kingProductionDatabase,
    data=data,
    tableName="prod_daily_allocated_volume"
)

wellData = joyn.getWellHeaderData(
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

read332hid = joyn.getWellObjectId(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
   nameOfWell="test_well"
 )

dataJoyn = joyn.getDailyAllocatedProductionRaw(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    wellHeaderData=wellData,
    daysToLookBack=2
)

tech.putDataReplace(
    server=kingLiveServer,
    database=kingProductionDatabase,
    data=dataJoyn,
    tableName="prod_joyn_data"
)



x = 5

# # AFE Stack WU-105
# afe.dailyCost(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerranchc302mh
# )
# afe.variance(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerranchc302mh
# )


# dateRange = pd.read_excel(r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\dateRanges.xlsx")

# masterGaugeDataList = []
# masterGaugeData = pd.DataFrame()

# for i in range(len(dateRange)):
#     startDate = dateRange["Start Date"][i]
#     endDate = dateRange["End Date"][i]
#     gaugeData = greasebook.getTankGauges(
#         greasebookApi=greasebookApi,
#         startDate=startDate,
#         endDate=endDate
#     )
#     masterGaugeDataList.append(gaugeData)

# masterGaugeData = pd.concat([masterGaugeData] + masterGaugeDataList, ignore_index=True)
# masterGaugeData["Date"] = pd.to_datetime(masterGaugeData["Date"])
# masterGaugeDataSorted = masterGaugeData.sort_values(by=["Tank ID", "Date"], ascending=[True, False])
# masterGaugeData.to_excel(r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\masterGaugeData.xlsx", index=False)

# wu108ProdData = king.getworlandUnit108Production(
#     numberOfDays=5
# )

# read332hid = joyn.getWellObjectId(
#     joynUsername=joynUsername,
#     joynPassword=joynPassword,
#     nameOfWell="test_well"
# )

# id = joyn.getJoynUsers(
#     joynUsername=joynUsername,
#     joynPassword=joynPassword,
#     nameToFind="mtanner"
# )

# readData = king.getReadHowardProduction()

# data = joyn.putReadData(
#         userId=id,
#         rawProductionData=readData,
#         objectId=read332hid,
#         joynUsername=joynUsername,
#         joynPassword=joynPassword
# )


data = combocurve.getLatestScenarioMonthly(
    projectIdKey="653fcf9ec230f38554b3f1c1",
    scenarioIdKey="653fd02e16729ef21ebd8f08",
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey
)

# crest = combocurve.ccScenarioToCrestFpSingleWell(
#     comboCurveScenarioData=data,
#     nglYield=noNglStream,
#     gasBtuFactor=1,
#     gasShrinkFactor=0,
#     oilPricePercent=1,
#     gasPricePercent=1,
#     nglPricePercent=.35,
#     oilVariableCost=0,
#     gasVariableCost=.5,
#     nglVariableCost=0,
#     waterVariableCost=.6,
#     state=texas,
#     capex=6600000,
# )

crestPdp = combocurve.ccScenarioToCrestFpPdp(
    comboCurveScenarioData=data,
    nglYield=noNglStream,
    gasBtuFactor=1,
    gasShrinkFactor=0,
    oilPricePercent=1,
    gasPricePercent=1,
    nglPricePercent=.3,
)


crestPdp.to_csv(r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\production\crestPdpJan2024Forecast.csv", index=False)

# # AFE Stack Miller Ranch B501MH
# afe.dailyCost(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerrancha501mh
# )
# afe.variance(
#     workingDataDirectory=kocDatawarehouse,
#     name=millerrancha501mh
# )

# data = pd.read_excel(r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\production\masterAllocatedProductionData.xlsx")

# #convert date to datetime
# data["Date"] = pd.to_datetime(data["Date"])

# data.to_excel(r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\production\masterAllocatedProductionDataDateTime.xlsx", index=False)

joyn.getJoynUser(
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    nameToFind="mtanner"
)

# Gets Browning 518H Production Data
browing518HProductionMonthtlyData = enverus.getWellProductionData(
    apiKey=enverusApiKey,
    wellApi14=browning5181H
)

# Allocate Wells From Greasebook
allocatedProductionData = greasebook.allocateWells(
    days=300,
    workingDataDirectory=kocDatawarehouse,
    greasebookApi=greasebookApi,
    pullProd=False,
    edgeCaseRollingAverage=7
)

# KOC Datawarehouse LIVE DUMP
print("Begin Exporting Greasebook Allocated Production Data to KOC Datawarehouse...")
allocatedProductionData.to_excel(
    kocDatawarehouse + r"\production\comboCurveAllocatedProductionJan12023.xlsx", index=False)
allocatedProductionData.to_json(
    kocDatawarehouse + r"\production\comboCurveAllocatedProductionJan12023.json", orient="records")
print("Finished Exporting Greasebook Allocated Production Data to KOC Datawarehouse!")

# JOYN STACK
# DAILY ALLOCATED PRODUCTION
joynData = joyn.getDailyAllocatedProduction(
    workingDataDirectory=kocDatawarehouse,
    joynUsername=joynUsername,
    joynPassword=joynPassword,
    daysToLookBack=2
)

print("Begin Exporting Master Joyn Data to KOC Datawarehouse...")
joynData.to_excel(kocDatawarehouse +
                  r"\production\masterJoynData.xlsx", index=False)
print("Finished Exporting Master Joyn Data to KOC Datawarehouse!")


# ComboCurve PUT Statements
combocurve.putGreasebookWellProductionData(
    workingDataDirectory=kocDatawarehouse,
    pullFromAllocation=False,
    serviceAccount=serviceAccount,
    comboCurveApi=comboCurveApiKey,
    greasebookApi=greasebookApi,
    daysToPull=daysToPull
)

combocurve.putJoynWellProductionData(
    allocatedProductionMaster=joynData,
    comboCurveApi=comboCurveApiKey,
    serviceAccount=serviceAccount
)


# AFE Stack Miller Ranch C301
afe.dailyCost(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchc301
)
afe.variance(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchc301
)

# AFE Stack Miller Ranch B501MH
afe.dailyCost(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchb501mh
)
afe.variance(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchb501mh
)

# AFE Stack Miller Ranch A501MH
afe.dailyCost(
    workingDataDirectory=kocDatawarehouse,
    name=millerrancha501mh
)
afe.variance(
    workingDataDirectory=kocDatawarehouse,
    name=millerrancha501mh
)

# AFE Stack Miller Ranch A502V
afe.dailyCost(
    workingDataDirectory=kocDatawarehouse,
    name=millerrancha502v
)
afe.variance(
    workingDataDirectory=kocDatawarehouse,
    name=millerrancha502v
)

# AFE Stack Miller Ranch A502V
afe.dailyCost(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchc302mh
)
afe.variance(
    workingDataDirectory=kocDatawarehouse,
    name=millerranchc302mh
)

# Combine AFE files and place in data warehouse
afe.combineAfeFiles(
    workingDataDirectory=kocDatawarehouse,
    listOfWells=listOfWells
)

print("Finished Running KOC Daily Scripts! You rock man")
