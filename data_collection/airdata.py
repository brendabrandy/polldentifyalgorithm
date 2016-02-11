"""
This code was used to generate download csv files and generate a group of
dataframes of pollutants. information comes from EPA air quality stations.
The daily data is then converted to monthly data. Returns final_summary
as the dataframe containing all the information required. Please go to
groupbylocation.py to obtain the code for altitude and state


However, if your computer does not have enough memory to process all the information,
please use singlepollutant.py to download all the files needed to generate specific dataframes
and use combine.py to combine all the dataframes.

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
from scipy.io import netcdf
from StringIO import StringIO
import pandas as pd
import numpy as np
import urllib2
from datetime import *
from dateutil.parser import parse
from pyspark.sql.types import *
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

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
    fields[16].name = "conc"
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
    df = sqlContext.createDataFrame(rdd_temp, schema)
    df_1 = df.toPandas()
    df_sorted = df_1.sort(["lon"])
    return df_sorted

#round off the longitude and latitude of datapoints of pollutants
#and convert it to the divisions given by uwnd and vwnd.
def rounding_off(num):
    rounded_num = num // 2.5
    intermediate = rounded_num * 2.5
    if (abs(num - intermediate) < 2.5):
        return intermediate
    else:
        return intermediate + 2.5

#organize data
summary_ozone = pd.DataFrame(columns = ['lat','lon','date','conc'])
summary_sulphur = pd.DataFrame(columns = ['lat','lon','date','conc'])
summary_carbon = pd.DataFrame(columns = ['lat','lon','date','conc'])
summary_nitrogen = pd.DataFrame(columns = ['lat','lon','date','conc'])
summary_pm10 = pd.DataFrame(columns = ['lat','lon','date','conc'])


#Assuming Spark Context defined as "sc"
#Loop through all the files and incorporate them into the files
for i in range (2000,2016):
    rdd44201 = unzip_file("http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/daily_44201_"+str(i)+".zip")
    df_ozone = RDD_to_Dataframe(rdd44201,44201)
    summary_ozone = pd.concat([summary_ozone,df_ozone])
    rdd42401 = unzip_file("http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/daily_42401_"+str(i)+".zip")
    df_sulphur = RDD_to_Dataframe(rdd42401,42401)
    summary_sulphur = pd.concat([summary_sulphur,df_sulphur])
    rdd42101 = unzip_file("http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/daily_42101_"+str(i)+".zip")
    df_carbon= RDD_to_Dataframe(rdd42101,42101)
    summary_carbon = pd.concat([summary_carbon,df_carbon])
    rdd42602 = unzip_file("http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/daily_42602_"+str(i)+".zip")
    df_nitrogen = RDD_to_Dataframe(rdd42602,42602)
    summary_nitrogen = pd.concat([summary_nitrogen,df_nitrogen])
    rdd81102 = unzip_file("http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/daily_81102_"+str(i)+".zip")
    df_pm10 = RDD_to_Dataframe(rdd81102,81102)
    summary_pm10 = pd.concat([summary_pm10,df_pm10])

summary_ozone['lon'] = summary_ozone['lon'].apply(rounding_off)
summary_ozone['lat'] = summary_ozone['lat'].apply(rounding_off)
summary_ozone = summary_ozone.set_index('date').groupby([pd.TimeGrouper('M'),'lat','lon'])['conc'].aggregate(np.mean)
summary_ozone = summary_ozone.to_frame()
summary_sulphur['lon'] = summary_sulphur['lon'].apply(rounding_off)
summary_sulphur['lat'] = summary_sulphur['lat'].apply(rounding_off)
summary_sulphur = summary_sulphur.set_index('date').groupby([pd.TimeGrouper('M'),'lat','lon'])['conc'].aggregate(np.mean)
summary_sulphur = summary_sulphur.to_frame()
summary_carbon['lon'] = summary_carbon['lon'].apply(rounding_off)
summary_carbon['lat'] = summary_carbon['lat'].apply(rounding_off)
summary_carbon = summary_carbon.set_index('date').groupby([pd.TimeGrouper('M'),'lat','lon'])['conc'].aggregate(np.mean)
summary_carbon = summary_carbon.to_frame()
summary_nitrogen['lon'] = summary_nitrogen['lon'].apply(rounding_off)
summary_nitrogen['lat'] = summary_nitrogen['lat'].apply(rounding_off)
summary_nitrogen = summary_nitrogen.set_index('date').groupby([pd.TimeGrouper('M'),'lat','lon'])['conc'].aggregate(np.mean)
summary_nitrogen = summary_nitrogen.to_frame()
summary_pm10['lon'] = summary_pm10['lon'].apply(rounding_off)
summary_pm10['lat'] = summary_pm10['lat'].apply(rounding_off)
summary_pm10 = summary_pm10.set_index('date').groupby([pd.TimeGrouper('M'),'lat','lon'])['conc'].aggregate(np.mean)
summary_pm10 = summary_pm10.to_frame()

summary_ozone.columns = ['O3']
summary_nitrogen.columns = ['NO2']
summary_carbon.columns = ['CO']
summary_sulphur.columns = ['SO2']
summary_pm10.columns = ['PM10']

final_summary = pd.concat([summary_ozone,summary_carbon,summary_sulphur,summary_nitrogen,summary_pm10],axis=1)
final_summary = final_summary.reset_index()
final_summary['alt'] = ""
final_summary['uwnd'] = ""
final_summary['vwnd'] = ""
final_summary[['lon','lat']]=final_summary[['lon','lat']].astype(float)
final_summary = final_summary[final_summary.date != "2015-12-30"]
