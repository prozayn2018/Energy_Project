import requests, time, csv, os, simplejson
from datetime import datetime
from google.cloud import bigquery


data_holder = []

#gets fludia data, will re-write to class and clean up later
def fludia_call_api(source_file_name):
    r = requests.get('https://app.wattspirit.com/api/smk/p1m/1611646056/1621232856',
    auth=('shelby.bons', 'wattspiritPS15'))

    data = r.json()


    for item in data:
        time = datetime.fromtimestamp(item[0]/1000).strftime("%m-%d-%Y %H:%M:%S")
        list_items = [time, item[1]]
        data_holder.append(list_items)
    #temporary csv file
    with open(source_file_name, 'w') as csvfile:
         fieldnames = ['date', 'watts']
          #create csv file
         csvwriter = csv.writer(csvfile)
          #write field
         csvwriter.writerow(fieldnames)
          #write row data
         csvwriter.writerows(data_holder)


#get data from https://www.eia.gov/opendata/
#https://www.eia.gov/electricity/gridmonitor/dashboard/daily_generation_mix/regional/REG-MIDW

def load_data_from_file(dataset_name, table_name, source_file_name):

    #construct BigQuery Client
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)

    #loading job_config
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True

    #reading csv file created earlier
    with open(source_file_name, 'rb') as source_file:
        # This example uses CSV, but you can use other formats.
        # See https://cloud.google.com/bigquery/loading-data
        job = bigquery_client.load_table_from_file(
            source_file, table, job_config=job_config)

    job.result()  # Wait for job to complete

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_name, table_name))

if __name__ == '__main__':
    #get fludia data
    fludia_call_api('fludia_data.csv')
    print('itworked')
    #wait for csv file to load
    time.sleep(5)
    #push data into BigQuery
    load_data_from_file(
        'energy_manager',
        'fludia_raw_daily',
        'fludia_data.csv'
        )
