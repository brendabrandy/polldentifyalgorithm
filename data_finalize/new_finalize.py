import pandas as pd
df_main = pd.read_csv("usable_summary.csv")
df_altitude = pd.read_csv("df_altitude.csv")

print df_main.dtypes

df_main[['state','city']]=df_main[['state','city']].astype(str)

print df_main.dtypes

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

#add in the locations that aren't scanned manually

print df_main
df_main.to_csv("usable_summary_final.csv")
