"""
This code was used to generate download csv files and generate a group of
dataframes of pollutants. information comes from EPA air quality stations.
The daily data is then converted to monthly data. Returns final_summary
as the dataframe containing all the information required. Please go to
groupbylocation.py to obtain the code for altitude and state


Code dictionary{
44201: Ozone
42401: Sulphur Dioxide
42101: Carbon Monoxide
42602: Nitrogen Dioxide
81102: PM 10
}
"""

from pyspark import SparkContext
from pyspark import SparkFiles
from zipfile import ZipFile
from StringIO import StringIO
import pandas as pd
import numpy as np
import urllib2
from datetime import *
from dateutil.parser import parse
from pyspark.sql.types import *
from pyspark.sql import SQLContext

sc = SparkContext("local","simple app")
ssc = SQLContext(sc)

pollutant_code = 81102
pollutant_name = "PM10"


#The function takes an zip file URL, and return an unzipped RDD
def unzip_file(path_to_zip_file):
    url = urllib2.urlopen(path_to_zip_file)
    zippy = ZipFile(StringIO(url.read()))
    path_to_unzipped_file = zippy.extract((ZipFile.namelist(zippy))[0])
    sc.addFile(path_to_unzipped_file)
    testFile = sc.textFile(path_to_unzipped_file, use_unicode=False)
    url.close()
    return testFile


#takes the unzipped pollution file, and returns dataframe that includes
#parameter code, longitude, latittude, arithmetic mean and date
#can only be used for pollution, not wind data
def RDD_to_Dataframe(rdd_file, type):
    header = rdd_file.first()
    schemaString = header.replace('"','')
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(',')]
    fields[5].dataType = FloatType()    #Latitude
    fields[6].dataType = FloatType()    #Longitude
    fields[11].dataType = TimestampType()   #date
    fields[16].dataType = FloatType()   #Arithmetic mean for the day
    fields[16].name = pollutant_name
    fields[5].name= "lat"
    fields[6].name= "lon"
    fields[11].name="date"
    reducefields = [fields[5],fields[6],fields[11],fields[16]]
    schema = StructType(reducefields)
    rddHeader = rdd_file.filter(lambda l: "State Code" in l)
    rddNoHeader = rdd_file.subtract(rddHeader)
    # using commas as delimiter and then extract the 5th, 6th, 11th and 16th field
    #data stored as float, float, date and float respectively
    rdd_temp = rddNoHeader.map(lambda k: k.split(",")).map(lambda p: (float(p[5]),float(p[6]),datetime.strptime(p[11].strip('"'),"%Y-%m-%d"),float(p[16])))
    df = ssc.createDataFrame(rdd_temp, schema)
    df_1 = df.toPandas()
    df_sorted = df_1.sort(["lon"])
    return df_sorted

#round off the longitude and latitude of datapoints of pollutants
#and convert it to the divisions given by uwnd and vwnd.
def rounding_off(num):
    num = float(num)
    rounded_num = num // 2.5
    intermediate = rounded_num * 2.5
    if (abs(num - intermediate) < 2.5):
        return intermediate
    else:
        return intermediate + 2.5


def to_year(date):
    return int(date.year)

def to_month(date):
    return int(date.month)

#organize data
summary_ozone = pd.DataFrame(columns = ['lat','lon','date',pollutant_name,'year','month'])

#Assuming Spark Context defined as "sc"
#Loop through all the files and incorporate them into the files
for i in range (2000,2016):
    rdd44201 = unzip_file("http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/daily_"+str(pollutant_code)+"_"+str(i)+".zip")
    df_ozone = RDD_to_Dataframe(rdd44201,pollutant_code)
    summary_ozone = pd.concat([summary_ozone,df_ozone])


summary_ozone['lon'] = summary_ozone['lon'].apply(rounding_off)
summary_ozone['lat'] = summary_ozone['lat'].apply(rounding_off)
summary_ozone['year'] = summary_ozone['date'].apply(to_year)
summary_ozone['month'] = summary_ozone['date'].apply(to_month)

summary_ozone[['lon','lat',pollutant_name]]=summary_ozone[['lon','lat',pollutant_name]].astype(float)
print summary_ozone.dtypes
times = pd.DatetimeIndex(summary_ozone['date'])
g1 = summary_ozone.groupby(['month','year','lat','lon'])[pollutant_name].mean()
summary_ozone = g1.to_frame()
summary_ozone.to_csv("summary_pm10.csv")

print summary_ozone
#final_summary = summary_ozone
#final_summary = final_summary.reset_index()
#final_summary[['lon','lat']]=final_summary[['lon','lat']].astype(float)
#final_summary = final_summary[final_summary.date != "2015-12-30"]
#
