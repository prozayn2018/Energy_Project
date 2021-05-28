import requests, time, csv, pprint
from datetime import datetime
from google.cloud import bigquery
import tokens

#global array for rows
row_holder = []

#api retry counter, 5 max:
eia_api_retry = 0

#gets net generation for primary energy producers, hourly, local-time
#in MwH
class eiaRequests():
    global row_holder

    def __init__(self, url_path, token, energy_name, series_id, time_st, time_end):
        self.url = url_path
        self.energy_name = energy_name
        self.token = token
        self.energy_source = series_id
        self.time_st = time_st
        self.time_end = time_end

    def eia_request(self):

        try:
            #r = requests.get('http://api.eia.gov/series/?api_key=64dffc30064e4eeff38e82dbb2395a2c&series_id=EBA.NW-ALL.NG.COL.HL&start=20210525T13-06&end=20210525T15-06', headers={'Content-Type': 'application/json'})
            r = requests.get(self.url + self.token + '&series_id=' + self.energy_source
            + '&start=' + self.time_st + '&end=' + self.time_end, headers={'Content-Type': 'application/json'})
            if(r.status_code == 200):
                pass
            elif(r.status_code != 200):
                print('API Request error: {0}'.format(r.status_code))
        except:
            pass
        data = r.json()

        char = '-'
        data_1 = data['series'][0]['data']
        for item in data_1:
            #print(item[0])
            item_1 = item[0]
            for i in range(len(item_1)):
                if(item_1[i] == char):
                    #this is timestamp in iso (new_string)
                    new_string = str(item_1[0:i])
                    #converts new_string to datetime object
                    time = datetime.strptime(new_string,'%Y%m%dT%H')
                    #formatting to correct datetime format
                    time_now = time.strftime('%m-%d-%Y %H:%M:%S')
                    #time_now = datetime.fromtimestamp(time).strftime('%m-%d-%Y %H:%M:%S.%f')
                    list_items = [time_now,item[1], self.energy_name]
                    row_holder.append(list_items)

#class dataTransform:

    #attributes for class

    #def __init__(self, data):
        self.data = data

    #def transformList(self):



if __name__ == '__main__':
    #loop here through eia_tokens and call eia_call each time
    #instance of eia API Call

    token_name = ['coal','hydro','natural_gas','nuclear','other','petroleum','solar','wind']
    for i in range(len(token_name)):
        energy_source = token_name[i]
        series_id = tokens.energy_source[energy_source]
        print(energy_source, series_id)
        #instance of eiaRequest
        eia_Request_call = eiaRequests(
                tokens.eia_url,
                tokens.eia_token,
                energy_source,
                series_id,
                '20210525T13-06',
                '20210525T15-06'
        )
        eia_Request_call.eia_request()
    pprint.pprint(row_holder)
