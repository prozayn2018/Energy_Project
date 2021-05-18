import requests
from datetime import datetime
import time
import csv
from pprint import pprint


data_holder = []

#gets fludia data, will re-write to class and clean up later
def fludia_call_api():
    r = requests.get('https://app.wattspirit.com/api/smk/p1m/1611615600/1621234861',
    auth=('shelby.bons', 'wattspiritPS15'))

    data = r.json()

    for item in data:
        time = datetime.fromtimestamp(item[0]/1000).strftime("%Y-%m-%d %I:%M:%S")
        list_items = [time, item[1]]
        data_holder.append(list_items)
    pprint(data_holder)

    with open('fludia_data' + '.csv', 'w') as csvfile:
          fieldnames = ['time', 'watts']
          #create csv file
          csvwriter = csv.writer(csvfile)
          #write field
          csvwriter.writerow(fieldnames)
          #write row data
          csvwriter.writerows(data_holder)

fludia_call_api()

#get data from https://www.eia.gov/opendata/
#https://www.eia.gov/electricity/gridmonitor/dashboard/daily_generation_mix/regional/REG-MIDW

def eia_call_api():
