import pandas as pd
df_main = pd.read_csv("final_summary.csv")
df_altitude = pd.read_csv("df_altitude.csv")
df_wind = pd.read_csv("wind.csv")

print df_main.dtypes

df_main[['state','city']]=df_main[['state','city']].astype(str)

print df_main.dtypes

for index,rows in df_main.iterrows():
    a = rows['lat']
    b = rows['lon']
    c = rows['month']
    d = rows['year']
    #Checks whether there is a value in df_latitude that matches the longitude and latitude final_summary
    ss = df_altitude[(df_altitude.lat == a) & (df_altitude.lon == b)]
    print ss
    if (len(ss) != 0):
    	#If it is a number, return the value to the cell
        ss1 = ss['alt']
        ss2 = ss['state']
        ss3 = ss['city']
        df_main.set_value(index,'alt',ss1 )
        df_main.set_value(index,'state',ss2)
        df_main.set_value(index,'city',ss3)

    ss4 = df_wind[(df_wind.lat == a) & (df_wind.lon == b) & (df_wind.month == int(c)) & (df_wind.year == int(d))]
    print ss4
    if (len(ss4) != 0):
        ss5 = ss4['uwnd'] #uwnd
        ss6 = ss4['vwnd'] #vwnd
        df_main.set_value(index,'uwnd',ss5)
        df_main.set_value(index,'vwnd',ss6)
#add in the locations that aren't scanned manually


df_main[['state','city']]=df_main[['state','city']].astype(str)

for index,rows in df_main.iterrows():
    a = rows['lat']
    b = rows['lon']
    #Checks whether there is a value in df_latitude that matches the longitude and latitude final_summary
    ss = df_altitude[(df_altitude.lat == a) & (df_altitude.lon == b)].values
    print ss
    if (len(ss) != 0):
    	#If it is a number, return the value to the cell
        ss2 = ss[0][3]
        ss3 = ss[0][4]
        df_main.set_value(index,'state',ss2)
        df_main.set_value(index,'city',ss3)

df_main.to_csv("usable_summary_final.csv")
