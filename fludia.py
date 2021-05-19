import requests, time, csv, os
from datetime import datetime
from google.cloud import bigquery
import tokens

#global_variable for data
data_holder = []

class API_Call_Fludia():

    def __init__(self, url_path, username, password ):
        #attributes for class
        self.url_path = url_path
        self.username = username
        self.password = password

    def fludia_call_api(self):
        #call api
        try:
            data = requests.get(self.url_path,
            auth=(self.username, self.password))
            print('HTTP Response code:', data.status_code)
        except (simplejson.errors.JSONDecodeError) as e:
            pass

        data_body = data.json()
        #append each row to data variable above
        for item in data_body:
            time = datetime.fromtimestamp(item[0]/1000).strftime("%m-%d-%Y %H:%M:%S")
            list_items = [time, item[1]]
            data_holder.append(list_items)

class Csv:

    #attributes for class
    def __init__(self, csv_file_name):
        self.csv_file_name = csv_file_name

    #temporary csv file
    def Csv_file_creator(self):

        with open(self.csv_file_name, 'w') as csvfile:
             fieldnames = ['date', 'watts']
              #create csv file
             csvwriter = csv.writer(csvfile)
              #write field
             csvwriter.writerow(fieldnames)
              #write row data
             csvwriter.writerows(data_holder)

class BigQuery():

    #attributes for class
    def __init__(self, dataset_name, table_name, source_file_name):
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.source_file_name = source_file_name

    def BQ_data_push(self):

        #construct BigQuery Client
        bigquery_client = bigquery.Client()
        dataset = bigquery_client.dataset(self.dataset_name)
        table = dataset.table(self.table_name)

        #loading job_config
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        #reading csv file created earlier
        with open(self.source_file_name, 'rb') as source_file:
            # This example uses CSV, but you can use other formats.
            # See https://cloud.google.com/bigquery/loading-data
            job = bigquery_client.load_table_from_file(
                source_file, table, job_config=job_config)

        job.result()  # Wait for job to complete

        print('Loaded {} rows into {}:{}.'.format(
            job.output_rows, self.dataset_name, self.table_name))

if __name__ == '__main__':
    #instance of Fludia API Call
    fludia_call = API_Call_Fludia(tokens.fludia_url, tokens.username, tokens.password)
    fludia_call.fludia_call_api()

    #instance of Csv
    Csv_creator = Csv('fludia_data.csv')
    Csv_creator.Csv_file_creator()

    #wait for csv file to load
    print('working on it....')
    time.sleep(5)

    #Instance of BigQuery
    BigQuery_call = BigQuery(
            'energy_manager',
            'fludia_raw_daily',
            'fludia_data.csv'
    )
    BigQuery_call.BQ_data_push()

    #google credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/zaynsagar/Energy_Proj/Energy_House/divine-fuze-223722-d9ac8cc5d808.json"
