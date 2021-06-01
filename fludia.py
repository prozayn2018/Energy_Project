import requests, time, csv, os
from datetime import datetime, timedelta
from google.cloud import bigquery
import tokens

#global_variable for data
data_holder = []
#row_data_holder for fludia_api_time
timestamp_argument_1 = None
#api retry counter, 5 max:
fludia_api_retry = 0

class API_Call_Fludia:

    def __init__(self, url_path, time_1, time_2,username, password ):
        #attributes for class
        self.url_path = url_path
        self.username = username
        self.password = password
        self.time_url_1 = str(time_1)
        self.time_url_2 = str(time_2)

    def fludia_call_api(self):

        import time, simplejson
        global fludia_api_retry

        base_url_path = self.url_path + '/' + self.time_url_1 + '/' + self.time_url_2
        print(base_url_path)

        #call api
        try:
            data = requests.get(base_url_path, headers={'Content-Type': 'application/json'},
            auth=(self.username, self.password))
            if (data.status_code == 200):
                print('HTTP Response code:', data.status_code)
            elif (data.status_code != 200):
                if (fludia_api_retry <= 5):
                    print('calling Fludia API Again, error: {0} | times re-tried: {1}'.format(data.status_code, fludia_api_retry))
                    fludia_api_retry += 1
                    time.sleep(5)
                    #will need to add arg here
                    fludia_call = API_Call_Fludia(tokens.fludia_url, self.time_url_1, self.time_url_2,tokens.username, tokens.password)
                    fludia_call.fludia_call_api()
                else:
                    pass

        except (simplejson.errors.JSONDecodeError) as e:
            pass

        data_body = data.json()

        #append each row to data variable above
        for item in data_body:
            conversion_time = item[0] / 1000.0
            time = datetime.fromtimestamp(conversion_time).strftime('%m-%d-%Y %H:%M:%S.%f')
            list_items = [time,item[0],float(item[1])]
            data_holder.append(list_items)
        print('Rows in dataholder:', len(data_holder))

class Csv:

    #attributes for class
    def __init__(self, csv_file_name):
        self.csv_file_name = csv_file_name

    #temporary csv file
    def Csv_file_creator(self):

        with open(self.csv_file_name, 'w') as csvfile:
             fieldnames = ['date','epoch_time','watts']
              #create csv file
             csvwriter = csv.writer(csvfile)
              #write field
             csvwriter.writerow(fieldnames)
              #write row data
             csvwriter.writerows(data_holder)
             print('SUCCESS: Fludia CSV File | Created')

class BigQuery:

    #attributes for class
    def __init__(self, dataset_name, table_name, source_file_name):
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.source_file_name = source_file_name


    def check_BQ_for_data(self):
        #checks bigquery for last row in table and
        #returns the last row plus one minute for calling fludia api again
        global timestamp_argument_1

        #construct BigQuery Client, will be used as global variables in funcs below
        bigquery_client = bigquery.Client()
        dataset = bigquery_client.dataset(self.dataset_name)
        table = dataset.table(self.table_name)

        #query into BigQuery fludia_raw_daily table to look for last item
        query_job = bigquery_client.query("""
        SELECT max(date)
        FROM energy_manager.fludia_raw_daily
        """)
        results = query_job.result()
        if results.total_rows == 0:
            pass
        else:
            for result in results:
                row_data = str(result[0])

                #will remove last occurrence of : from timestamp returned by BQ
                char = [':', '.']
                length = len(row_data)
                for i in range(length):
                    if(row_data[i] == char[0]):
                        row_data_2 = row_data[0:i] + row_data[i+1:length]
                row_data_3 = row_data_2.replace('+', '.')

                #removing '.' from timestamp for epoch format
                length_data_3 = len(row_data_3)
                for i in range(length_data_3):
                    if(row_data_3[i] == char[1]):
                        last_row_in_bigquery = row_data_3[0:i]

                #converting datetime object to epoch
                date_time_obj = datetime.strptime(last_row_in_bigquery, '%Y-%m-%d %H:%M:%S')
                #adding 60 seconds to datetime for next api call
                #plus 360 mins because time is in UTC, we are behind 6 hours, so add 360 mins
                api_call_time = date_time_obj + timedelta(minutes=360, seconds=60)
                timestamp_argument_1 = (api_call_time - datetime(1970, 1, 1)).total_seconds()
                print(timestamp_argument_1)
                return timestamp_argument_1



    def BQ_data_push(self):

        from google.oauth2 import service_account

        ## Explicitly use service account credentials by specifying the private key file.
        credentials = service_account.Credentials.from_service_account_file(
        '/Users/zaynsagar/Energy_Proj/Energy_House/divine-fuze-223722-d9ac8cc5d808.json')

        #construct BigQuery Client
        bigquery_client = bigquery.Client()
        dataset = bigquery_client.dataset(self.dataset_name)
        table = dataset.table(self.table_name)

        #loading job_config
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        #if csv data has rows then run below

        #reading csv file created earlier
        with open(self.source_file_name, 'rb') as source_file:
            # This example uses CSV, but can use other formats.
            # See https://cloud.google.com/bigquery/loading-data
            job = bigquery_client.load_table_from_file(
                source_file, table, job_config=job_config)

        job.result()  # Wait for job to complete

        print('Loaded {} rows into {}:{}.'.format(
            job.output_rows, self.dataset_name, self.table_name))

class Helper:

    @staticmethod
    #this needs to be local
    #returns time now but in GMT NOT Local
    def current_time():
        time_now = datetime.now()
        current_time = time_now.strftime('%Y-%m-%d %H:%M:%S')
        #current_time_1 = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        date_time_obj = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S') + timedelta(minutes=360)
        return date_time_obj

    @staticmethod
    #this needs to be local
    def datetime_to_epoch():
        time_now = Helper.current_time()
        current_epoch_time = (time_now - datetime(1970, 1, 1)).total_seconds()
        #cur_epoch_time_no_zero = Helper.string_strip(str(current_epoch_time), '.')
        print('api_call_2:',current_epoch_time)
        return current_epoch_time
        #return cur_epoch_time_no_zero

    @staticmethod
    #strips a string
    def string_strip(a_string, char):
        for i in range(len(a_string)):
            if(a_string[i] == char):
                new_string = a_string[0:i]
                print('api_call_1:', new_string)
                return new_string


if __name__ == '__main__':

    #check to see if record exists in BQ:

    #Instance of BigQuery
    BigQuery_call = BigQuery(
            'energy_manager',
            'fludia_raw_daily',
            'fludia_data.csv'
    )
    #gets last record time + 1 min from table 'fludia_raw_daily'
    time_1 = BigQuery_call.check_BQ_for_data()

    if timestamp_argument_1:
        Helper_call = Helper()
        #converting float epoch to str without additional zero at end
        previous_epoch_time = Helper.string_strip(str(timestamp_argument_1), '.')
        cur_epoch_time = Helper_call.datetime_to_epoch()

        #instance of Fludia API Call
        fludia_call = API_Call_Fludia(tokens.fludia_url, previous_epoch_time,cur_epoch_time ,tokens.username, tokens.password)
        fludia_call.fludia_call_api()

        #instance of Csv
        Csv_creator = Csv('fludia_data.csv')
        Csv_creator.Csv_file_creator()
        #wait for csv file to load
        time.sleep(5)

        if len(data_holder) != 0:
            #Instance of BigQuery
            BigQuery_call.BQ_data_push()

            #google credentials
            os.remove('fludia_data.csv')
