import pandas as pd
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext

sc = SparkContext("local","simple app")
ssc = SQLContext(sc)

#read all related csv files to dataframes
summary_ozone = pd.read_csv("summary_ozone.csv")
summary_carbon = pd.read_csv("summary_carbon.csv")
summary_sulphur = pd.read_csv("summary_sulphur.csv")
summary_nitrogen = pd.read_csv("summary_nitrogen.csv")
summary_pm10 = pd.read_csv("summary_pm10.csv")

#concatenate all frames together to form one final summary
final_summary_0 = pd.merge(summary_ozone,summary_carbon,how = "outer",on = ['month','year','lat','lon'])
final_summary_1 = pd.merge(summary_sulphur,summary_nitrogen,how = "outer",on = ['month','year','lat','lon'])
final_summary_2 = pd.merge(final_summary_1,summary_pm10,how = "outer",on = ['month','year','lat','lon'])
final_summary = pd.merge(final_summary_0,final_summary_2,how = "outer",on = ['month','year','lat','lon'])
final_summary = final_summary.reset_index()

#propare other columns for future additions of altitudes, wind, state and city
final_summary['alt'] = ""
final_summary['uwnd'] = ""
final_summary['vwnd'] = ""
final_summary['state'] = ""
final_summary['city'] = ""
print final_summary
#deliver the summary as a csv files
final_summary.to_csv("final_summary.csv")
