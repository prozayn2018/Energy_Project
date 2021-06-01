import requests, time, csv, pprint
from datetime import datetime, timedelta
from google.cloud import bigquery
import tokens
import fludia as fl


#global array for rows
row_holder = []

#row_data_holder for fludia_api_time
timestamp_argument_1 = []

#time returned for database

time_returned = []

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

                    #calling c02 emissions function
                    MwH = float(item[1])
                    #cO2_total_pounds = (MwH * (1000 * 2.21))
                    #print(cO2_total_pounds)

                    cO2_call = self.CO2Emissions(self.energy_name, MwH)
                    list_items = [time_now,item[1], cO2_call[0],cO2_call[1],self.energy_name]
                    row_holder.append(list_items)

    def CO2Emissions(self,energy_name, MWh):

        #per kwh how much in gCO2eq/KWH
        #this is estimation using public models
        #number at first index is min, at last index is max
        #exp: min nuclear is 6, max is 26, average is 32/2 = 16

        emissions_source = {
        "coal": [800, 1002],
        "natural_gas": [430, 517],
        "petroleum": [894, 966],
        "hydro": [2, 13],
        "nuclear": [6, 26],
        "wind": [3,38],
        "solar": [75,116],
        "other": [300, 500]
        }

        #other in Colorado is Petro, Nuclear and Biomass (mainly Petro)
        cO2_total_grams = (MWh * (1000 * ((emissions_source[energy_name][0] + emissions_source[energy_name][1]) / 2)))
        cO2_total_pounds = round(cO2_total_grams * 0.002204, 2)
        return (int(cO2_total_grams), int(cO2_total_pounds))

class Csv:

    #attributes for class

    def __init__(self, csv_file_name):
        self.csv_file_name = csv_file_name

    #temporary csv file
    def Csv_file_creator(self):

        with open(self.csv_file_name, 'w') as csvfile:
             fieldnames = ['date','MWh', 'cO2_grams_emission','cO2_pounds_emission' ,'energy_source']
              #create csv file
             csvwriter = csv.writer(csvfile)
              #write field
             csvwriter.writerow(fieldnames)
              #write row data
             csvwriter.writerows(row_holder)
             print('SUCCESS: Eia CSV File | Created')

class BigQuery:
    global time_returned
    #same as fludia.py, carry that class over here

    #attributes for class
    def __init__(self, dataset_name, table_name, source_file_name):
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.source_file_name = source_file_name


    def check_BQ_eia_data(self):

        #construct BigQuery Client, will be used as global variables in funcs below
        bigquery_client = bigquery.Client()
        dataset = bigquery_client.dataset(self.dataset_name)
        table = dataset.table(self.table_name)

        #query into BigQuery fludia_raw_daily table to look for last item
        query_job = bigquery_client.query("""
        SELECT max(date)
        FROM energy_manager.eia_raw_hourly
        """)
        results = query_job.result()
        if results.total_rows == 0:
            pass
        else:
            for result in results:
                row_data = str(result[0])
                time_returned.append(row_data)
                call_format = self.formatTime()

    #converts string datetime into datetime then isoformat
    def formatTime(self):

        to_date_iso = time_returned[0]
        #will remove extra time until char
        char = '+'
        for i in range(len(to_date_iso)):
            if(to_date_iso[i] == char):
                row_data_2 = to_date_iso[0:i]

        time = datetime.strptime(row_data_2,'%Y-%m-%d %H:%M:%S') + timedelta(minutes=60)
        #time is now in iso
        #call helper to pass time_1 in iso
        Helper_call = Helper()
        time_1 = Helper_call.api_time_1(time.isoformat())
        timestamp_argument_1.append(time_1)
        #self.api_time_1(time.isoformat())

class Helper:

    @staticmethod
    #this needs to be local
    #returns time now but in GMT NOT Local
    def current_time():
        time_now = datetime.now()
        current_time = time_now.strftime('%Y-%m-%d %H:%M:%S')
        date_time_obj = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
        time_to_iso = date_time_obj.isoformat()
        return (date_time_obj.isoformat())

    def api_time_1(self, time_beg):

        #format '20180701T01-06'
        #current 2018-07-01T23:00:00

        char = ':'
        for i in range(len(time_beg)):
            if (time_beg[i] == char):
                data = time_beg[0:i]
        for i in range(len(data)):
            if(data[i] == char):
                data_2 = data[0:i]

        time_data = data_2.replace('-', '')
        return time_data

if __name__ == '__main__':

    #Instance of BigQuery
    BigQuery_call = BigQuery(
            'energy_manager',
            'fludia_raw_daily',
            'fludia_data.csv'
    )

    #gets most recent row time and returns time needed for next call
    BigQuery_call.check_BQ_eia_data()

    if timestamp_argument_1:
        #Instance of helper, getting time arg two for url call
        Helper_call = Helper()
        time_2 = Helper_call.current_time()
        url_time_2 = Helper_call.api_time_1(time_2)
        timestamp_argument_1.append(url_time_2)

        #instance of eia API Call
        token_name = ['coal','hydro','natural_gas','nuclear','other','petroleum','solar','wind']
        for i in range(len(token_name)):
            energy_source = token_name[i]
            series_id = tokens.energy_source[energy_source]
            #print(energy_source, series_id)
            #instance of eiaRequest
            eia_Request_call = eiaRequests(
                    tokens.eia_url,
                    tokens.eia_token,
                    energy_source,
                    series_id,
                    timestamp_argument_1[0] + '-06',
                    timestamp_argument_1[1] + '-06'
            )
            eia_Request_call.eia_request()
        #pprint.pprint(row_holder)

        Csv_creator = Csv('eia_data.csv')
        Csv_creator.Csv_file_creator()
        #wait for csv file to load
        time.sleep(5)

        #importing Bigquery data push from fludia.py
        #pushes data into BigQuery
        BQ_data = fl.BigQuery('energy_manager',
        'eia_raw_hourly',
        'eia_data.csv')
        BQ_data.BQ_data_push()
