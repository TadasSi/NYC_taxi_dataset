from pyspark.sql import SparkSession
import datetime
import sys
import tqdm
import os
import subprocess


def get_spark_session(session_name):
    # creates spark session
    sc = (SparkSession.builder
          .appName(session_name)
          .config("hive.exec.dynamic.partition", "true")
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .config("parquet.compression", "SNAPPY")
          .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .enableHiveSupport()
          .getOrCreate())
    return sc


def run_cmd(args_list):
    """
    runs hdfs shell script commands
    """
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode


def write_parquet(df, hdfs_path, file_name, schema):
    """write dataframe to parquet file"""
    print(str(datetime.datetime.now()) + " Write file to parquet: {}".format(file_name))
    output_name = hdfs_path + file_name
    df.write \
        .mode('Overwrite') \
        .option("schema", schema) \
        .partitionBy("dt").parquet(output_name)


def create_tmp_local_dir(dir_name):
    directory = os.path.dirname(dir_name)
    if not os.path.exists(directory):
        print("Directory {} does not exist, creating it...".format(directory))
        try:
            os.makedirs(directory)
        except Exception as e:
            print(e)
            print("Could not create directory, exiting...")
            sys.exit(1)


def empty_tmp_local(dir_name):
    date_now = str(datetime.datetime.now())
    if len(os.listdir(dir_name)) == 0:
        print("Directory {} is empty".format(dir_name))
    else:
        print("Removing files from {} directory...".format(dir_name))
        process = subprocess.Popen(''' rm ''' + dir_name + '''* ''', shell=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = process.communicate()
        if process.returncode == 0:
            print(date_now + " Files were removed.")
        else:
            print(std_err)
            print(date_now + " Could not clear tmp directory, exiting...")


def move_local_to_hdfs(local_dir, local_file, hdfs_path):
    """
    moves local file to hdfs
    """
    date_now = str(datetime.datetime.now())
    cmd = ['hadoop', 'fs', '-moveFromLocal', '-f', local_dir + local_file, hdfs_path]
    process = run_cmd(cmd)

    if process == 0:
        print(date_now + " Files were moved to HDFS.")
    else:
        print(date_now + " Move to HDFS failed")
        empty_tmp_local(local_dir)


def file_exists(request, file_name, local_dir):
    total_file_size = int(request.headers.get('content-length', 0))
    if os.path.exists(os.path.join(local_dir, file_name)):
        local_filename_size = os.path.getsize(os.path.join(local_dir, file_name))
        already_exists = True if total_file_size == local_filename_size else False
    else:
        already_exists = False
    return already_exists


def hdfs_file_exists(hdfs_path, file_name):
    cmd = ['hdfs', 'dfs', '-ls', hdfs_path + file_name]
    process = run_cmd(cmd)
    if process == 0:
        already_exists = True
    else:
        already_exists = False
    return already_exists


def write_file_chunks(request, file_name, local_dir):
    """
    write while to local directory
    move file to hdfs
    """
    total_file_size = int(request.headers.get('content-length', 0))
    with open(os.path.join(local_dir, file_name), 'wb') as f:
        for chunk in tqdm.tqdm(request.iter_content(chunk_size=1024000), total=total_file_size, unit='KB',
                               unit_scale=True):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


def get_weekday(value):
    date = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    date_num = date.weekday()
    if date_num == 0:
        return "Mon"
    if date_num == 1:
        return "Tue"
    if date_num == 2:
        return "Wed"
    if date_num == 3:
        return "Thu"
    if date_num == 4:
        return "Fri"
    if date_num == 5:
        return "Sat"
    if date_num == 6:
        return "Sun"


def get_weekend(value):
    date = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    date_num = date.weekday()
    if 0 <= date_num <= 4:
        return "Weekday"
    elif 5 <= date_num <= 6:
        return "Weekend"
