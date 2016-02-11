
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext
import json

from pyspark.sql.types import *
from pyspark.sql import Row
import pandas as pd
import pyspark.mllib
import pyspark.mllib.regression
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.feature import StandardScaler
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import *
from pyspark.mllib.regression import LinearRegressionWithSGD

sc = SparkContext("local", "Simple Appy")
sqlContext = SQLContext(sc)

dff = pd.read_csv("../src/csv/to_matrix.csv")
series_lat = dff.groupby(['lat','lon']).groups.keys()
pollutant_array = ['O3','SO2','NO2','CO','PM10']
new_dict = dict()

#rdd = sc.textFile('../src/csv/to_matrix.csv')
#rdd = rdd.map(lambda line: line.split(","))
#header = rdd.first()
#rdd = rdd.filter(lambda line:line != header)
#df = rdd.map(lambda line: Row(month = float(line[2]), year=line[3],
#lat=line[4], lon=line[5], O3=float(line[6]), CO=float(line[7]), SO2=float(line[8]),
#NO2=float(line[9]), PM10=float(line[10]),uwnd=float(line[12]),vwnd=float(line[13]))).toDF()
#print df.show(5)
#print df.dtypes


df = sqlContext.createDataFrame(data = dff)

for items in series_lat:
    if (new_dict.has_key(items[0]) != True):
        new_dict[items[0]] = {}

    current_dict= new_dict[items[0]]
    if (current_dict.has_key(items[1]) != True):
        current_dict[items[1]] = {}
    current_dict = current_dict[items[1]]

    df_gg = df[(df.lat == items[0]) & (df.lon == items[1])]
    #needs to add one more column for time
    for pollutant in pollutant_array:
        if (pollutant == 'O3'):
            df_ggg = df_gg.select(df_gg.month,df_gg.uwnd,df_gg.vwnd,df_gg.O3) #should be able to change O3 later
        elif (pollutant == 'SO2'):
            df_ggg = df_gg.select(df_gg.month,df_gg.uwnd,df_gg.vwnd,df_gg.SO2)
        elif (pollutant == 'CO'):
            df_ggg = df_gg.select(df_gg.month,df_gg.uwnd,df_gg.vwnd,df_gg.CO)
        elif (pollutant == 'NO2'):
            df_ggg = df_gg.select(df_gg.month,df_gg.uwnd,df_gg.vwnd,df_gg.NO2)
        else:
            df_ggg = df_gg.select(df_gg.month,df_gg.uwnd,df_gg.vwnd,df_gg.PM10)


        print df_ggg.dtypes
        print df_ggg.show(5)

        temp = df_ggg.map(lambda line:LabeledPoint(line[3],[line[0:3]]))
        #features = df_ggg.map(lambda row: row[0:3])
        #standardizer = StandardScaler()
        #model = standardizer.fit(features)
        #features_transform = model.transform(features)

        #lab = df_ggg.map(lambda row: row[3])

        #transformedData = lab.zip(features_transform)
        #transformedData = transformedData.map(lambda row: LabeledPoint(row[0],[row[1]]))
        #transformedData = temp.map(lambda row: LabeledPoint(row[0],[row[1]]))

        trainingData, testingData = temp.randomSplit([.8,.2],seed=1234)
        linearModel = LinearRegressionWithSGD.train(trainingData,1000,.2,intercept = True)
        current_dict[pollutant+"m1"] = linearModel.weights[0] #month
        current_dict[pollutant+"m2"] = linearModel.weights[1] #uwnd
        current_dict[pollutant+"m3"] = linearModel.weights[2] #vwnd
        current_dict[pollutant+"c"] = linearModel.intercept #intercept

with open('realtimey.json', 'w') as fp:
    json.dump(new_dict, fp)
