## Developer: Gabe Tatman
## Date: 2024-02-28
## Description: This script will send out a daily report for a specific group of wells to a desired email distribution list


from dotenv import load_dotenv
from kingscripts.analytics import king


import os
import datetime as dt

# Getting Date Variables
dateToday = dt.datetime.today()
function = "daily_reporting"

## ECHO UNIT WELLS

## Get file locations for the wells
pathtoEchoUnit2250 = str(os.getenv("ECHO_UNIT_2250"))
pathToEchoUnit2251 = str(os.getenv("ECHO_UNIT_2251"))
pathToEchoUnit2252 = str(os.getenv("ECHO_UNIT_2252"))

# Get the data for the wells
echoUnit2250Data = king.getConocoEchoUnit(
    pathToFolder=pathtoEchoUnit2250,
    daysToLookBack=10
)

echoUnit2251Data = king.getConocoEchoUnit(
    pathToFolder=pathToEchoUnit2251,
    daysToLookBack=10
)

echoUnit2252Data = king.getConocoEchoUnit(
    pathToFolder=pathToEchoUnit2252,
    daysToLookBack=10
)


dateEnd = dt.datetime.today()

runtimeSeconds = (dateEnd - dateToday).total_seconds()

usageFunction = king.updateUsageStatsEtlRuntime(
    etlStartTime=dateToday,
    etlEndTime=dateEnd,
    function=function,
    runtime=runtimeSeconds
)

