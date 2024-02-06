# Week 2 Homework
Data Engineering Zoomcamp 2024

## Green Taxi ETL
Pushing green taxi data to PostgreSQL and Google Cloud Storage bucket

### Data Loader (wk_02_load_api_data)
```python
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
     
    
    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'trip_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float,
                    'ehail_fee':float
                }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    
    df_list = []

    for month in [10, 11, 12]:
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz'
        file = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        df_list.append(file)
        
    df = pd.concat(df_list, ignore_index=True)  

    
    
    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

```

### Transformer (wk_02_transform_taxi_data)

```python
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("Rows with zero passengers: ", data['passenger_count'].isin([0]).sum())
    print("Rows with zero trip distance: ", data['trip_distance'].isin([0]).sum())

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    data['lpep_dropoff_date'] = data['lpep_dropoff_datetime'].dt.date
    
    # col_rename_count =  0
    # new_columns = {}  # Store the new column names
    # 
    # for col in data.columns:
    #     new_col = ''
    #     for i, char in enumerate(col):
    #         if char.isupper() and i !=  0:  # Skip the first character
    #             new_col += '_' + char.lower()
    #         else:
    #             new_col += char
    #     new_columns[col] = new_col.strip('_')  # Remove leading underscore if present
    #     if new_col != col:
    #         col_rename_count +=  1
    
    # data.rename(columns=new_columns, inplace=True)  # Apply the new column names
    # print(f'Renamed {col_rename_count} columns to snake case.')
    # print(new_columns)

    data = data.rename(columns={'PULocationID': 'pu_location_id', 'VendorID': 'vendor_id', 'DOLocationID': 'do_location_id', 'RatecodeID': 'ratecode_id'})


    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0) ] # return data


@test
def test_output(output, *args):
    assert output['passenger_count'].isin([0]).sum() ==  0, 'There are rides with zero passengers.'
    assert output['trip_distance'].isin([0]).sum() ==  0, 'There are rides with zero trip distances.'  # Updated message
    assert output['vendor_id'].count() > 0, 'There are Vendor ID\'s in the dataset'
```

### Data Exporter to PostgreSQL (wk_02_taxi_data_to_postgres)

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'ny_taxi'  # Specify the name of the schema to export data to
    table_name = 'wk_02_green_taxi_data'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

```

### Data Exporter to Google Cloud Storage bucket (wk_02_taxi_to_gcs_partitioned_parquet)

```python
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/creds.json'
bucket_name = 'mage-dezoomcamp-wk02'
project_id = 'mage-dezoomcamp-wk02'

table_name = 'wk_02_green_nyc_taxi_data'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    # data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
```