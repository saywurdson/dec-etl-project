import duckdb
import sys
import os

# create the connection
con = duckdb.connect('/workspaces/synpuf-etl/data/omop.db')
print("Database connection established.")

# create schema
con.execute('create schema if not exists synpuf')

# create tables
tables = [
    'care_site', 'condition_occurrence', 'death', 'device_cost',
    'device_exposure', 'drug_cost', 'drug_exposure', 'location',
    'measurement_occurrence', 'observation_period', 'observation',
    'payer_plan_period', 'person', 'procedure_cost', 'procedure_occurrence',
    'provider', 'specimen', 'visit_cost', 'visit_occurrence'
]

arg = sys.argv[1]
while arg != 'overwrite' and arg != 'append':
    print('Invalid argument. Please enter either "overwrite" or "append".')
    arg = input()

for table in tables:
    for i in range(1, 21):
        csv_file = f'{table}_{i}.csv'
        file_path = f'/workspaces/synpuf-etl/data/BASE_OUTPUT_DIRECTORY/{csv_file}'
        if os.path.exists(file_path) and file_path.endswith('.csv'):
            print(f'Updating table: {table}...')
            if arg == 'overwrite':
                con.execute(f'create or replace table synpuf.{table} as select * from read_csv_auto("{file_path}", ignore_errors=1)')
            elif arg == 'append':
                con.execute(f'insert into synpuf.{table} select * from read_csv_auto("{file_path}", ignore_errors=1)')
            print(f'Table {table} updated.')
        else:
            print(f'File {csv_file} not found or is not a csv file.')


# close the connection
con.close()
print("Database connection closed.")