##  This script runs the 3 main ETL scripts for the kingscripts
##  includes etl_combocurve, etl_scada, and etl_joyn
##
##  Version 3.4.0
##  Developer: Michael Tanner, Gabe Tatman

import datetime as dt
from kingscripts.analytics import king

function = "etl_main"

print("Starting etl_main")

dateNow = dt.datetime.now()
print("The Start Time is: ", dateNow)

import etl_scada
import etl_joyn
import etl_combocurve
import daily_reporting

## calculate time to run
dateEnding = dt.datetime.today()

runtimeSeconds = (dateEnding - dateNow).total_seconds()

usageFunction = king.updateUsageStatsEtlRuntime(
    etlStartTime=dateNow,
    etlEndTime=dateEnding,
    function=function,
    runtime=runtimeSeconds
)

dateFinished = dt.datetime.now()
print("The End Time is: ", dateFinished)

#print time in hours, minutes, and seconds between start and end
print("The total time to run the ETL is: ", dateFinished - dateNow)

usageStat = king.updateUsageStatsEtl(dateNow)

print("ETL Process Complete")