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
    https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet
    """

    partial_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-"

    green_taxi_data = []
    for month in range(1,13):
        url = partial_url + ''.join(str(month)).zfill(2)
        url = url + ".parquet"
        print(url)
        taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID': pd.Int64Dtype(),
                    'store_and_fwd_flag': str,
                    'PULocationID': pd.Int64Dtype(),
                    'DOLocationID': pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra': float,
                    'mta_tax': float,
                    'tip_amount': float,
                    'tolls_amount': float,
                    'improvement_surcharge': float,
                    'total_amount': float,
                    'congestion_surcharge': float
                    }

        date_columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
        df = pd.read_parquet(url)
        df = df.astype(taxi_dtypes)

        for col in date_columns:
            df[col] = pd.to_datetime(df[col])

        green_taxi_data.append(df)

    full_df = pd.concat(green_taxi_data)

    return full_df