import requests
from datetime import datetime
import time
import csv
from google.cloud import bigquery
import os

data_holder = []

client = language.LanguageServiceClient.from_service_account_json("/Users/zaynsagar/Energy_Proj/Energy_House/divine-fuze-223722-d9ac8cc5d808.json")
#gets fludia data, will re-write to class and clean up later
def fludia_call_api():
    r = requests.get('https://app.wattspirit.com/api/smk/p1m/1611615600/1621234861',
    auth=('shelby.bons', 'wattspiritPS15'))

    data = r.json()

    for item in data:
        time = datetime.fromtimestamp(item[0]/1000).strftime("%Y-%m-%d %I:%M:%S")
        list_items = [time, item[1]]
        data_holder.append(list_items)
    #temporary csv file
    with open('fludia_data' + '.csv', 'w') as csvfile:
         fieldnames = ['time', 'watts']
          #create csv file
         csvwriter = csv.writer(csvfile)
          #write field
         csvwriter.writerow(fieldnames)
          #write row data
         csvwriter.writerows(data_holder)


#get data from https://www.eia.gov/opendata/
#https://www.eia.gov/electricity/gridmonitor/dashboard/daily_generation_mix/regional/REG-MIDW

def load_data_from_file(dataset_name, table_name):

    #construct BigQuery Client
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)

    # Reload the table to get the schema.
    table.reload()

    #table_id = 'My_First_Project.energy_manager.fludia_raw_daily'
    #loading job_config
    with open(source_file_name, 'rb') as source_file:
        # This example uses CSV, but you can use other formats.
        # See https://cloud.google.com/bigquery/loading-data
        job = table.upload_from_file(
            source_file, source_format='text/csv')

    job.result()  # Wait for job to complete

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_name, table_name))

if __name__ == '__main__':
    #get fludia data
    fludia_call_api()
    #push data into BigQuery
    load_data_from_file(
        'energy_manager',
        'fludia_raw_daily',
        )
