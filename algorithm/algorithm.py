'''
This file contains the general algorithm for manipulating the final dataframe.
For a location (x,y,z), the location would have pollution concentration C and the rate
of source emission is Q

Hence, for a location (x,y,z), if influenced by multiple sources of pollution,
C = a_0 * Q_0 + a_1 * Q_1 + a_2 * Q_2 + ... + a_n * Q_n, where a is the coefficeint
calculated by a gaussian dispersion model and Q is the rate of emission from location n

Functions used:
a: uses the Gaussian dispersion model to return a coefficient for a location
check_sigma_y: calculate sigma_y in the gaussian dispersion model
check_sigma_z: calculate sigma_z in the gaussian dispersion model

Possible bug:
there is a row of zeros, rendering the matrix impossible to solve
either sigma_y or sigma_z ends up being zero, but this was avoided already

Be very careful when writing the dataframe to it, check whether the A and B actually
have their elements filled in properly.

Also make sure that the matrix has the proper dates. i.e. when you pass a dataframe
into the matrix, that dataframe should only include one single date and one single kind
of pollutant.
'''

#return coefficients to each place and and source for ONE SINGLE MONTH
#if the matrix is written in the form Ax = B, where A is the coefficient,
#B is the final pollutant concentration C, and x is the rate of emission Q


import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt, atan2
df_main = pd.read_csv("./src/to_matrix.csv")

#drop first two unused columns
df_main = df_main.drop(df_main.columns[[0,1]],axis = 1)

#make 5 new columns for recording the sources
df_main['source_O3'] = ""
df_main['source_NO2'] = ""
df_main['source_SO2'] = ""
df_main['source_CO'] = ""
df_main['source_PM10'] = ""

pollutant_list = ['O3','NO2','SO2','CO','PM10']
year_list = [2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015]
month_list = [1,2,3,4,5,6,7,8,9,10,11,12]

def matrix(df,year,month,pollutant):
    df = df[df['year'] == year]
    df = df[df['month'] == month]

    df = df.reset_index()
    total_rows = len(df)

    A = np.zeros((total_rows,total_rows))
    B = np.zeros(total_rows)

    #This for loop is used to populate the matrix A and B
    for i in range(0,total_rows): #c1, c2 ..., initial
        final = [df.loc[i,'lon'],df.loc[i,'lat'],df.loc[i,'alt']]
        B.itemset(i,df.loc[i,pollutant])
        for j in range(0,total_rows): #q1, q2 ...
            init = [df.loc[j,'lon'],df.loc[j,'lat'],df.loc[j,'alt']]
            coefficient = a(df.loc[j,'uwnd'],df.loc[j,'vwnd'],init[0],init[1], init[2],final[0],final[1],final[2])
            A.itemset((i,j),coefficient)

    #solve the matrix
    X = np.linalg.solve(A,B)
    #Save as percentages and save it in matrix A

    for c in range(0,total_rows):
        if (X[c] < 0):
            X[c] = abs(X[c])
        for d in range(0,total_rows):
            if (A[c,d] < 0):
                A[c,d] = abs(A[c,d])

    for m in range(0,total_rows):
        A[m] = np.multiply(A[m],X)
        sum_m = np.sum(A[m])
        for n in range(0,total_rows):
            new_value = (A[m,n]/sum_m)*100
            A.itemset((m,n),new_value)

    my_list = []
    #make a series with all the rows, state names and
    for b in range(0,total_rows):
        new_series = pd.Series(data = A[b], index = df.city)
        grouppy = new_series.groupby(new_series.index).sum()
        grouppy_dict = grouppy.to_dict()
        item_del = []
        for item in grouppy_dict:
            if (grouppy_dict[item] < 0.0001):
                item_del.append(item)
        for deletes in item_del:
            del grouppy_dict[deletes]
        listty = [grouppy_dict,df.loc[b,'index']]
        my_list.append(listty)
    #returns
    return my_list

#Calculate the coefficient for source Q to place with concentration C
#d is the directio vector between source and receptor
#u is the wind vector
def a(u_x, u_y, init_lon, init_lat, init_z, final_lon, final_lat, final_z):
    #init[0] / final[0]: longitude
    #init[1] / final[1]: latitude
    #init[2] / final[2]: altitude


    if ((init_lat == final_lat) and (init_lon == final_lon)):
        haver = haversine(init_lon, init_lat, init_lon + 2.5, init_lat+2.5)
    else:
        #convert lat lon to cartesian coordinates
        haver = haversine(init_lon, init_lat, final_lon, final_lat)
    dx = haver['dist'] * cos(haver['bearing'])
    dy = haver['dist'] * sin(haver['bearing'])

    #vector distance between initial and final location
    d = [dx, dy, (final_z - init_z)]
    #calculate y
    dot_product = u_x*d[0] + u_y*d[1]


    #if (dot_product < 0):
        #return 0 #if the wind blows in opposite direction of the displacement vector, return 0
    #else:
    mag_u = np.sqrt(u_x**2 + u_y**2)
    mag_d = np.sqrt(d[0]**2 + d[1]**2)
    if ((mag_d == 0) or (dx == 0)):
        sigma_y = 90 #assuming sigma is classified as D - Neutral
        sigma_z = 5
        y = 0
    else:
        theta = np.arccos(dot_product/(mag_u*mag_d))
        y = mag_d * np.sin(theta) # y displacement from place a to place b
        #sigma y and z
        downwind_x = d[0]
        sigma_y = check_sigma_y(mag_u,abs(dx))
        sigma_z = check_sigma_z(mag_u,abs(dx))

    #other value constants
    z = final_z
    H = init_z

    denominator = 2*np.pi * sigma_y * sigma_z * mag_u
    numerator = np.exp((-0.5*y**2/sigma_y**2)+(-0.5*(z-H)**2/sigma_z**2))
    ans = (numerator / denominator)
    return ans

#compute sigma_z
def check_sigma_z(wind_speed,downwind_x):
    wind = abs(wind_speed)
    if (wind < 2):          #A-B
        if(downwind_x<10000):
            return sigma((0.495+0.310)/2,downwind_x,(0.873+0.897)/2)
        else:
            return sigma((0.606+0.523)/2,downwind_x,(0.851+0.840)/2)
    elif (wind <3):     #B
        if(downwind_x<=10000):
            return sigma(0.310,downwind_x,0.897)
        else:
            return sigma(0.523,downwind_x,0.840)
    elif (wind < 5):   #C
        if(downwind_x<=10000):
            return sigma(0.197,downwind_x,0.908)
        else:
            return sigma(0.285,downwind_x,0.867)
    elif(wind < 6):     #C_D
        if(downwind_x<=10000):
            return sigma((0.197+0.122)/2,downwind_x,(0.908+0.916)/2)
        else:
            return sigma((0.285+0.193)/2,downwind_x,(0.867+0.865)/2)
    else:#D
        if(downwind_x<=10000):
            return sigma(0.122,downwind_x,0.916)
        else:
            return sigma(0.193,downwind_x,0.865)


#compute sigma_y
def check_sigma_y(wind_speed,downwind_x):
    wind = abs(wind_speed)
    if (wind < 2):          #A-B
        if (downwind_x<= 500):
            return sigma((0.0383+1.393)/2,downwind_x,(1.281+0.9467)/2)
        else:
            return sigma((0.0002539+0.04936)/2,downwind_x,(2.089+1.114)/2)
    elif (wind <3):     #B
        if (downwind_x<= 500):
            return sigma(1.393,downwind_x,0.9467)
        else:
            return sigma(0.04936,downwind_x,1.114)
    elif (wind < 5):    #C
        if (downwind_x<= 500):
            return sigma(0.1120,downwind_x,0.9100)
        elif (downwind_x<=5000):
            return sigma(0.1014,downwind_x,0.926)
        else:
            return sigma(0.1154,downwind_x,0.9109)
    elif(wind < 6):     #C_D
        if (downwind_x<= 500):
            return sigma((0.1120+0.0856)/2,downwind_x,(0.9100+0.8650)/2)
        elif (downwind_x<=5000):
            return sigma((0.1014+0.2591)/2,downwind_x,(0.926+0.6869)/2)
        else:
            return sigma((0.1154+0.7368)/2,downwind_x,(0.9109+0.5642)/2)
    else:#D
        if (downwind_x<= 500):
            return sigma(0.0856,downwind_x,0.8650)
        elif (downwind_x<=5000):
            return sigma(0.2591,downwind_x,0.6869)
        else:
            return sigma(0.7368,downwind_x,0.5642)

def sigma(constant, distance, power):
    return constant * (distance ** power)

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    bearing = atan2(sin(lon2-lon1)*cos(lat2), cos(lat1)*sin(lat2)*sin(lat1)*cos(lat2)*cos(lon2-lon1))
    bearing = bearing + 2*np.pi
    while (bearing > 2*np.pi):
        bearing -= np.pi

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371
    dist = c*r
    return {'dist' : dist, 'bearing' : bearing}

for year in year_list:
    for month in month_list:
        if (year == 2015 and (month > 9)):
            break
        else:
            for pollutant in pollutant_list:
                print "year: ", year, " month: ", month, " pollutant: ", pollutant
                #calculate the sources of pollution
                my_list = matrix(df_main,year,month,pollutant)
                index = 0
                while(index < len(my_list)):
                    label = 'source_'+ pollutant
                    df_main.set_value(my_list[index][1],label,my_list[index][0])
                    index += 1

print df_main
df_main.to_csv("./src/after_matrix.csv")
#Calculate the coefficient for source Q to place with concentration C
#d is the directio vector between source and receptor
#u is the wind vector
