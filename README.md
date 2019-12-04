# NYC_taxi_dataset
The aim of this project is to process NYC Taxi Trip Record Data.

https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

•  Load raw data files to hdfs.

•  Transform them and write to parquets.

# How to prepare for running

1. Download main load_file.py, utils.py and config.json files.
2. Set your own variables in config.json file (hdfs paths, local directories, etc).
3. Run script

### How to run
Set variables:

PAR_ENV='prod';

PAR_CONFIG_PATH={path to config json}'config.json';

Use this spark command to run script:

```
{path to spark dir i.e} /spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
   --master yarn \
   --deploy-mode cluster \
   --conf "spark.pyspark.python={path to python i.e.} /spark240python3/bin/python" \
   --conf "spark.pyspark.driver.python={path to python driver i.e.} /spark240python3/bin/python" \
   ``{path to main load_file.py file}`` /load_file.py $PAR_ENV $PAR_CONFIG_PATH
```
