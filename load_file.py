#######
# process NYC Taxi Trip Record Data #
######
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, TimestampType, DecimalType
import pyspark.sql.functions as F
import datetime
import requests
import utils
import json
import sys

# read values from config file
env = sys.argv[1]
config_path = sys.argv[2]

with open(config_path) as config_file:
    config_data = json.load(config_file)
url = config_data["environment"][env]["source_url"]
hdfs_raw_path = config_data["environment"][env]["hdfs_raw_path"]
hdfs_path = config_data["environment"][env]["hdfs_path"]
local_dir = config_data["environment"][env]["hdfs_path"]

# to_do move to launch file
# yellow or green
taxi_color = "yellow"
year = 2019
month = 3
overwrite_file = False


def define_schema(src_name):
    """
    define source file's schema
    """
    return {
        'yellow':
            StructType([
                StructField('corrupted', StringType(), True),
                StructField('VendorID', StringType(), True),
                StructField('tpep_pickup_datetime', TimestampType(), True),
                StructField('tpep_dropoff_datetime', TimestampType(), True),
                StructField('passenger_count', IntegerType(), True),
                StructField('trip_distance', DecimalType(10, 4), True),
                StructField('RatecodeID', StringType(), True),
                StructField('store_and_fwd_flag', StringType(), True),
                StructField('PULocationID', StringType(), True),
                StructField('DOLocationID', StringType(), True),
                StructField('payment_type', StringType(), True),
                StructField('fare_amount', DecimalType(10, 2), True),
                StructField('extra', DecimalType(10, 2), True),
                StructField('mta_tax', DecimalType(10, 2), True),
                StructField('tip_amount', DecimalType(10, 2), True),
                StructField('tolls_amount', DecimalType(10, 2), True),
                StructField('improvement_surcharge', DecimalType(10, 2), True),
                StructField('total_amount', DecimalType(10, 2), True),
                StructField('congestion_surcharge', DecimalType(10, 2), True)
            ]),
        'green':
            StructType([
                StructField('corrupted', StringType(), True),
                StructField('VendorID', StringType(), True),
                StructField('lpep_pickup_datetime', TimestampType(), True),
                StructField('lpep_dropoff_datetime', TimestampType(), True),
                StructField('store_and_fwd_flag', StringType(), True),
                StructField('RatecodeID', StringType(), True),
                StructField('PULocationID', StringType(), True),
                StructField('DOLocationID', StringType(), True),
                StructField('passenger_count', IntegerType(), True),
                StructField('trip_distance', DecimalType(10, 4), True),
                StructField('fare_amount', DecimalType(10, 2), True),
                StructField('extra', DecimalType(10, 2), True),
                StructField('mta_tax', DecimalType(10, 2), True),
                StructField('tip_amount', DecimalType(10, 2), True),
                StructField('tolls_amount', DecimalType(10, 2), True),
                StructField('ehail_fee', DecimalType(10, 2), True),
                StructField('improvement_surcharge', DecimalType(10, 2), True),
                StructField('total_amount', DecimalType(10, 2), True),
                StructField('payment_type', StringType(), True),
                StructField('trip_type', StringType(), True),
                StructField('congestion_surcharge', DecimalType(10, 2), True)
            ])
    }[src_name]


def write_schema(src_name):
    """
    define outgoing file's schema
    """
    return {
        'yellow':
            StructType([
                StructField('VendorID', StringType(), True),
                StructField('tpep_pickup_datetime', TimestampType(), True),
                StructField('tpep_dropoff_datetime', TimestampType(), True),
                StructField('passenger_count', IntegerType(), True),
                StructField('trip_distance', DecimalType(10, 4), True),
                StructField('RatecodeID', StringType(), True),
                StructField('store_and_fwd_flag', StringType(), True),
                StructField('PULocationID', StringType(), True),
                StructField('DOLocationID', StringType(), True),
                StructField('payment_type', StringType(), True),
                StructField('fare_amount', DecimalType(10, 2), True),
                StructField('extra', DecimalType(10, 2), True),
                StructField('mta_tax', DecimalType(10, 2), True),
                StructField('tip_amount', DecimalType(10, 2), True),
                StructField('tolls_amount', DecimalType(10, 2), True),
                StructField('improvement_surcharge', DecimalType(10, 2), True),
                StructField('total_amount', DecimalType(10, 2), True),
                StructField('congestion_surcharge', DecimalType(10, 2), True),
                StructField('file_name', StringType(), True),
                StructField('update_date', TimestampType(), True),
                StructField('dt', StringType(), True)
            ]),
        'green':
            StructType([
                StructField('VendorID', StringType(), True),
                StructField('lpep_pickup_datetime', TimestampType(), True),
                StructField('lpep_dropoff_datetime', TimestampType(), True),
                StructField('store_and_fwd_flag', StringType(), True),
                StructField('RatecodeID', StringType(), True),
                StructField('PULocationID', StringType(), True),
                StructField('DOLocationID', StringType(), True),
                StructField('passenger_count', IntegerType(), True),
                StructField('trip_distance', DecimalType(10, 4), True),
                StructField('fare_amount', DecimalType(10, 2), True),
                StructField('extra', DecimalType(10, 2), True),
                StructField('mta_tax', DecimalType(10, 2), True),
                StructField('tip_amount', DecimalType(10, 2), True),
                StructField('tolls_amount', DecimalType(10, 2), True),
                StructField('ehail_fee', DecimalType(10, 2), True),
                StructField('improvement_surcharge', DecimalType(10, 2), True),
                StructField('total_amount', DecimalType(10, 2), True),
                StructField('payment_type', StringType(), True),
                StructField('trip_type', StringType(), True),
                StructField('congestion_surcharge', DecimalType(10, 2), True),
                StructField('file_name', StringType(), True),
                StructField('update_date', TimestampType(), True),
                StructField('dt', StringType(), True)
            ]),
        'corrupted':
            StructType([
                StructField('corrupted', StringType(), True),
                StructField('file_name', StringType(), True),
                StructField('update_date', TimestampType(), True)
            ])
    }[src_name]


def transform_file(file_path, schema, partition, spark, file_name):
    print(str(datetime.datetime.now()) + " Transform file: {}".format(file_path + file_name))
    update_date = datetime.datetime.now()

    df_source_file = spark.read \
        .option("delimiter", ",") \
        .option("quote", "\"") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("columnNameOfCorruptRecord", "corrupted") \
        .csv(file_path + file_name, schema=schema).cache()

    df_transformed = df_source_file \
        .filter(F.col("corrupted").isNull()) \
        .withColumn("file_name", F.lit(file_name).cast('string')) \
        .withColumn("update_date", F.lit(update_date).cast('timestamp')) \
        .withColumn("dt", F.lit(partition).cast('string')) \
        .drop(F.col("corrupted"))

    df_corrupted = df_source_file \
        .select(F.col("corrupted").alias("corrupted").cast('string'),
                F.lit(file_name).alias("file_name").cast('string'),
                F.lit(update_date).alias("update_date").cast('timestamp')) \
        .filter(F.col("corrupted").isNotNull())

    return df_transformed, df_corrupted


def download_file(local_dir, file_name, overwrite=False):
    """download file from url """
    dataset_url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/{}'.format(file_name)

    request = requests.get(dataset_url, stream=True)
    print(str(datetime.datetime.now()) + " Start downloading file: {}".format(file_name))
    if overwrite:
        utils.write_file_chunks(request, file_name, local_dir)
    else:
        if utils.file_exists(request, file_name, local_dir):
            print("File {} already downloaded.".format(file_name))
        else:
            utils.write_file_chunks(request, file_name, local_dir)
    print(str(datetime.datetime.now()) + " Downloaded file: {}".format(file_name))

    return file_name


def load_file_hdfs(local_dir, hdfs_path, file_name, overwrite=False):
    utils.create_tmp_local_dir(local_dir)
    hdfs_file_exist = utils.hdfs_file_exists(hdfs_path, file_name)
    if (overwrite & hdfs_file_exist) | ((not overwrite) & (not hdfs_file_exist)):
        file_name = download_file(local_dir=local_dir, file_name=file_name, overwrite=overwrite)
        utils.move_local_to_hdfs(local_dir=local_dir, local_file=file_name, hdfs_path=hdfs_path)


def main():
    print("Starting at " + str(datetime.datetime.now()))

    file_name = '{}_tripdata_{}-{}.csv'.format(taxi_color, year, str(month).zfill(2))
    dt_partition = '{}-{}-01'.format(year, str(month).zfill(2))

    spark = utils.get_spark_session("ny_taxi")

    load_file_hdfs(local_dir=local_dir, hdfs_path=hdfs_raw_path, file_name=file_name, overwrite=overwrite_file)

    df_transform, df_corrupted = \
        transform_file(hdfs_raw_path, schema=define_schema(taxi_color),
                       partition=dt_partition, spark=spark, file_name=file_name)

    utils.write_parquet(df=df_transform, hdfs_path=hdfs_path, file_name='{}_tripdata'.format(taxi_color),
                        schema=write_schema(taxi_color))

    if df_corrupted.count() != 0:
        utils.write_parquet(df=df_corrupted, hdfs_path=hdfs_path, file_name='{}_corrupted'.format(taxi_color),
                            schema=write_schema("corrupted"))

    print("Finished at " + str(datetime.datetime.now()))


if __name__ == "__main__":
    main()
