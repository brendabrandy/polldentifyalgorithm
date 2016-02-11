'''
This code is used to place related wind data into a summary dataframe. The uwnd and vwnd obtained comes from NOAA:
ftp://ftp.cdc.noaa.gov/Datasets/ncep.reanalysis.dailyavgs/surface/ . The nc file is converted to csv files for
Apache Spark through matlab. The matlab file is saved as convert.m .
'''

import requests, StringIO, pandas as pd, json, re
import numpy as np

content_string_u = get_file_content(credentials_u)
uwnd = pd.read_csv(content_string_u, names = ['lon','lat','time','uwnd'])

content_string_v = get_file_content(credentials_v)
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
new_df.set_index()

for index,rows in final_summary.iterrows():
    a = rows['lat']
    b = rows['lon']
    c = rows['date'].month
    d = rows['date'].year
    #Checks whether there is a value in df_latitude that matches the longitude and latitude final_summary
    ss = new_df[(new_df.lat == a) & (new_df.lon == b) & (new_df.month == c) & (new_df.year == d)].values
    if (len(ss) != 0):
        #If it is a number, return the value to the cell
        ss1 = ss[0][3]
        ss2 = ss[0][4]
        ss3 = ss[0][5] #month
        ss4 = ss[0][6] #year
        final_summary.set_value(index, 'uwnd',ss1)
        final_summary.set_value(index, 'vwnd',ss2)
    else:
        #if there is no number, then return Not a NUmber for the element
        final_summary.set_value(index,'uwnd','NaN')
        final_summary.set_value(index,'vwnd','NaN')

def get_file_content(credentials):
    '''For given credentials, this functions returns a StringIO object containg the file content.'''

    url1 = ''.join([credentials['auth_url'], '/v3/auth/tokens'])
    data = {'auth': {'identity': {'methods': ['password'],
            'password': {'user': {'name': credentials['username'],'domain': {'id': credentials['domain_id']},
            'password': credentials['password']}}}}}
    headers1 = {'Content-Type': 'application/json'}
    resp1 = requests.post(url=url1, data=json.dumps(data), headers=headers1)
    resp1_body = resp1.json()
    for e1 in resp1_body['token']['catalog']:
        if(e1['type']=='object-store'):
            for e2 in e1['endpoints']:
                if(e2['interface']=='public'and e2['region']==credentials['region']):
                    url2 = ''.join([e2['url'],'/', credentials['container'], '/', credentials['filename']])
    s_subject_token = resp1.headers['x-subject-token']
    headers2 = {'X-Auth-Token': s_subject_token, 'accept': 'application/json'}
    resp2 = requests.get(url=url2, headers=headers2)
    return StringIO.StringIO(resp2.content)

credentials_u = {
    'auth_url': 'https://identity.open.softlayer.com',
    'domain_id': '24ea5bc355c340fb89109ee990246e8c',
    'username': 'admin_490197b2-3772-4817-9fb0-414179ab8ac2_0889ddcf4d99',
    'region' : 'dallas',
    'password': 'I?tu,#ngLV=FT98I',
    'filename': 'uwnd_mon_mean.csv',
    'container': 'test_store'
}

credentials_v = {
    'auth_url': 'https://identity.open.softlayer.com',
    'domain_id': '24ea5bc355c340fb89109ee990246e8c',
    'username': 'admin_490197b2-3772-4817-9fb0-414179ab8ac2_0889ddcf4d99',
    'region' : 'dallas',
    'password': 'I?tu,#ngLV=FT98I',
    'filename': 'vwnd_mon_mean.csv',
    'container': 'test_store'
}
