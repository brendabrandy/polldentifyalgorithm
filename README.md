# Polldentify algorithms

Polldentify is a web app that tracks and traces the sources of pollution within a US city base on its geographical location and wind direction at a moment in history. It compiles 15 years of data, run all the data through an algorithm inspired by the Gaussian Dispersion Model of Pollution, and showcased the info through http://polldentify.mybluemix.net/#/ .This github open sources all the programs that we use to collect, compile and analyze the data

##Data_collection

airdata.py: crawls concentration of pollutants (ozone, sulphur dioxide, nitrogen dioxide, carbon monoxide, PM10) from the EPA and save it as a dataframe (can be converted to csv through pd.to_csv).

singledata.py: in case your computer doesn't have enough memory for all the pollutant files downloaded through airdata.py, this allows you to download each pollutant one at a time. 

openfile.m: MATLAB program to download nc files on wind data from NOAA and save it as a csv file.

wind_extract.py / wind_extract_final.py: extract wind data from file generated from openfile.m

groupbylocation.py: downloads altitude information at each sampling longitude and latitude point through GoogleAPI and save it as csv file.

combine.py: combine wind data, altitude, longitude, latitude and air pollutant data into one final dataframe, indexed by date and save it as a csv file.

##Data_finalize

fillinna.py: in case of missing data, we either interpolate the data or use a randomize function to put a best possible value for the data.

frame_finalize.py / new_finalize.py: finalize the dataframe so that it could be directly delivered into an algorithm.

##Algorithms
algorithm.py: This file contains the general algorithm for manipulating the final dataframe. For a location (x,y,z), the location would have pollution concentration C and the rate of source emission is Q

new_linearreg.py: linear regression algorithm to predict pollutant concentration in the future. Open for improvements since linear regression is a very basic machine learning algorithm.
