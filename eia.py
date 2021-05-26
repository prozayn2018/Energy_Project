import requests, time, csv, os, simplejson, json, pprint
from datetime import datetime
from google.cloud import bigquery

def eia_call_api():

    try:
        r = requests.get('http://api.eia.gov/series/?api_key=64dffc30064e4eeff38e82dbb2395a2c&series_id=EBA.NW-ALL.NG.COL.HL&start=20210525T13-06&end=20210525T15-06', headers={'Content-Type': 'application/json'})
    except:
        pass
    data = r.json()
    #pprint.pprint(data)
    #data is coming back in local time (mountain)
    list_holder = []
    char = '-'
    data_1 = data['series'][0]['data']
    for item in data_1:
        print(item[0])
        item_1 = item[0]
        for i in range(len(item_1)):
            if(item_1[i] == char):
                new_string = str(item_1[0:i])
                list_items = [new_string,item[1]]
                list_holder.append(list_items)
    print(list_holder)



        #if item == 'series':
            #y = data[item].get('data')
            #pprint.pprint(data[item][0])
            #pprint.pprint(y)

eia_call_api()
