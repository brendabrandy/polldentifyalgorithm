import pandas as pd
from scipy.interpolate import interp2d
import numpy as np
from random import uniform

df_main = pd.read_csv("usable_summary_final.csv")
df_main = df_main.drop(df_main.columns[[0, 1, 2, 3]], axis=1)
df_main = df_main.dropna(subset=['alt', 'uwnd', 'vwnd'])
df_main = df_main.reset_index()

def change_pos(num):
    if num < 0:
        return (num + 360)

year_list = [2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015]
month_list = [1,2,3,4,5,6,7,8,9,10,11,12]
pollutant_list = ['O3','SO2','NO2','CO','PM10']
for year in year_list:
    print year
    for month in month_list:
        print month
        if (year == 2015 and (month > 9)):
            break
        else:
            df_change = df_main[(df_main.year == year) & (df_main.month == month)]
            for pollutant in pollutant_list:
                #making the function
                print pollutant
                df_change = df_change.dropna(subset = [pollutant])
                x = df_change['lat']
                y = df_change['lon'].apply(change_pos)
                maximum = np.amax(df_change[pollutant])
                minimum = np.amin(df_change[pollutant])
                z = df_change[pollutant]
                function = interp2d(x, y, z, kind='linear')

                #apply function to rows with NaN
                for index,rows in df_main.iterrows():
                    if ((rows['year'] == year) & (rows['month'] == month)):
                        if (np.isnan(rows[pollutant]) == True):
                            new_value = function(rows['lat'],rows['lon'])
                            df_main.set_value(index,pollutant,new_value)
                        if (np.isnan(rows[pollutant]) == True):
                            new_value = uniform(minimum,maximum)
                            df_main.set_value(index,pollutant,new_value)
                        if (rows['O3'] < 0):
                            new_value = abs(rows['O3'])
                            df_main.set_value(index,pollutant,new_value)
#need to delete 2015/10/11/12



print df_main
df_main.to_csv("to_matrix.csv")
