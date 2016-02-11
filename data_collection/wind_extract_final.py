import pandas as pd
import numpy as np
content_string_u = "uwnd_mon_mean.csv"
content_string_v = "vwnd_mon_mean.csv"

uwnd = pd.read_csv(content_string_u, names = ['lon','lat','time','uwnd'])
vwnd = pd.read_csv(content_string_v,names = ['lon','lat','time','vwnd'])

#Altering data on the uwnd and vwnd such that it matches the US coordinate system
#US has latitude between N 25 and N 50, and longitude between E 170 and E 310
new_uwnd = uwnd[uwnd.uwnd != 0]
new_uwnd = new_uwnd[uwnd.lon >= 170.0]
new_uwnd = new_uwnd[uwnd.lon <= 310.0]
new_uwnd = new_uwnd[uwnd.lat >= 17.5]
new_uwnd = new_uwnd[uwnd.lat <= 50.0]

new_vwnd = vwnd[vwnd.vwnd != 0]
new_vwnd = new_vwnd[vwnd.lon >= 170.0]
new_vwnd = new_vwnd[vwnd.lon <= 310.0]
new_vwnd = new_vwnd[vwnd.lat >= 17.5]
new_vwnd = new_vwnd[vwnd.lat <= 50.0]

#unincluded states: Hawaii, DC, Alaska, Delaware, Maryland,


#Combine the dataframes together and take away the redundant columns
new_df = pd.concat([new_uwnd, new_vwnd], axis = 1,keys = ['time','lon','lat'],ignore_index =True)
del new_df[4]
del new_df[5]
del new_df[6]
new_df.columns = ['lon','lat','time','uwnd','vwnd']



years=2000
month = 1
new_df['month'] = ""
new_df['year'] = ""
for index,rows in new_df.iterrows():
    if rows['lon'] > 180:
        new_lon = rows['lon'] - 360
        new_df.set_value(index,'lon',new_lon)
    new_df.set_value(index,'month',month)
    new_df.set_value(index,'year',years)
    month += 1
    if(month == 13):
        month=1
        years += 1
    if (rows['time'] == 1892664):
        month = 1
        years = 2000

del new_df['time']
new_df.to_csv("wind.csv")
