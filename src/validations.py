import sys

from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, BooleanType, ArrayType

print(sys.version)
from pyspark.sql.functions import col
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import boto3
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
import datetime
from datetime import datetime
import json
from types import SimpleNamespace
from pyspark.sql.types import DateType, DoubleType, StringType, IntegerType
from pyspark.sql.functions import input_file_name
import time

start_time = time.time()
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
logger = glueContext.get_logger()
logger.info("record-validation-poc-job started")
logger.info(f"sys.argv = {sys.argv}")  # for better debugging we output everything we got from the outside
job_meta_args = getResolvedOptions(sys.argv, ["JOB_NAME"])
run_meta = {
    "JOB_ID": job_meta_args["JOB_ID"],
    "JOB_NAME": job_meta_args["JOB_NAME"],
    "JOB_RUN_ID": job_meta_args["JOB_RUN_ID"]
}

myargs = getResolvedOptions(sys.argv, [
    "my_region_name",
    "raw_bucket_name",
    "nsc_staging_bucket_name",
    "my_source_bucket_name",
    "my_sns_topicarn_error",
    "config_bucket_name"
])

region_name = myargs["my_region_name"]
raw_bucket_name = myargs["raw_bucket_name"]
nsc_staging_bucket_name = myargs["nsc_staging_bucket_name"]
source_bucket_name = myargs["my_source_bucket_name"]
sns_topicarn_error = myargs["my_sns_topicarn_error"]
config_bucket_name = myargs["config_bucket_name"]

file_name = "s3://data-validation-poc/raw-data-folder/sales-records.csv"


def get_json_from_s3(bucket_name, key_name):
    s3 = boto3.client('s3')
    data = s3.get_object(Bucket=bucket_name, Key=key_name)['Body'].read()
    return json.loads(data, object_hook=lambda d: SimpleNamespace(**d))


def write_summary(message):
    s3 = boto3.client('s3')
    s3.put_object(
        Body=json.dumps(message),
        Bucket=config_bucket_name,
        Key=("validation_summary/record-validation-summary-" + datetime.utcnow().strftime('%Y%m%d%H%M%S') + ".json")
    )


def is_valid_date(date_string, format_string):
    try:
        return bool(datetime.strptime(date_string, format_string))
    except ValueError:
        return False


def validate_date(data_map, column_schema, validation):
    date_as_string = data_map[column_schema.index]
    format_str = column_schema.date_format
    if len(date_as_string.strip()) > 0:
        if not is_valid_date(date_as_string.strip(), format_str):
            return [{
                'error_code': validation.error_code,
                'validation_name': validation.validation_name,
                'message': 'Not a valid date',
                'column_name': column_schema.name,
                'input_value': date_as_string,
                'date_format_expected': format_str
            }]


def validate_Integer(data_map, column_schema, validation):
    data_str = data_map[column_schema.index]
    if len(data_str.strip()) > 0:
        if not data_str.strip().isdigit():
            return [{
                'error_code': validation.error_code,
                'validation_name': validation.validation_name,
                'message': 'Not a valid Integer',
                'column_name': column_schema.name,
                'input_value': str(data_str)
            }]


def is_float(data_str):
    try:
        float(data_str)
        return True
    except ValueError:
        return False


def validate_double_type(data_map, column_schema, validation):
    data_str = data_map[column_schema.index]
    if len(data_str.strip()) > 0:
        if not is_float(data_str.strip()):
            return [{
                'error_code': validation.error_code,
                'validation_name': validation.validation_name,
                'message': 'Not a valid floating point number',
                'column_name': column_schema.name,
                'input_value': str(data_str)
            }]


def validate_data_types(rec_map, validation):
    errors = []
    for column_schema in schema.columns:
        if column_schema.data_type == "DateType":
            error_lst = validate_date(rec_map, column_schema, validation)
            if error_lst:
                errors.extend(error_lst)
        elif column_schema.data_type == "IntegerType":
            error_lst = validate_Integer(rec_map, column_schema, validation)
            if error_lst:
                errors.extend(error_lst)
        elif column_schema.data_type == "DoubleType":
            error_lst = validate_double_type(rec_map, column_schema, validation)
            if error_lst:
                errors.extend(error_lst)
    return errors


def is_null(data_str):
    if data_str == None or str(data_str).strip() == "":
        return True
    else:
        return False


def validate_mandatory_columns(rec_map, validation):
    errors = []
    for column_schema in schema.columns:
        if not column_schema.nullable:
            column_value = rec_map[column_schema.index]
            if is_null(column_value):
                error = [{
                    'error_code': validation.error_code,
                    'validation_name': validation.validation_name,
                    'message': 'column is not nullable',
                    'column_name': column_schema.name,
                    'input value': str(column_value)
                }]
                errors.extend(error)
    return errors


def validate_delimiter(rec_map, validation):
    if len(schema.columns) != len(rec_map):
        return [{
            'error_code': validation.error_code,
            'validation_name': validation.validation_name,
            'message': 'number of columns present in data does not matching with no of columns in schema',
            'number_of_columns_in_schema': str(len(schema.columns)),
            'number_of_columns_in_data': str(len(rec_map))
        }]
    else:
        None


@udf(returnType=ArrayType(MapType(StringType(), StringType())))
def validate(record_map):
    errors = []
    for validation in validation_manifest.validations:
        if validation.enable == True:
            if validation.validation_name == "validate_delimiter":
                error_lst = validate_delimiter(record_map, validation)
                if error_lst:
                    errors.extend(error_lst)
            elif validation.validation_name == "validate_data_types":
                error_lst = validate_data_types(record_map, validation)
                if error_lst:
                    errors.extend(error_lst)
            elif validation.validation_name == "validate_mandatory_columns":
                error_lst = validate_mandatory_columns(record_map, validation)
                if error_lst:
                    errors.extend(error_lst)
    return errors


schema = get_json_from_s3(config_bucket_name, "configs/schema.json")
validation_manifest = get_json_from_s3(config_bucket_name, "configs/validations.json")


def send_notification(message):
    boto3session = boto3.Session(region_name=region_name)
    sns_client = boto3session.client('sns', verify=False)
    response = sns_client.publish(
        TargetArn=sns_topicarn_error,
        Message=json.dumps({'default': json.dumps(message)}),
        Subject='Validation result',
        MessageStructure='json'
    )


@udf(returnType=MapType(IntegerType(), StringType()))
def get_data_map(raw_string):
    lst = raw_string.split(",")
    return dict(enumerate(lst))


@udf(returnType=BooleanType())
def is_valid(validation_result):
    return len(validation_result) == 0


raw_dataframe = spark.read.text(file_name)

validated_df = raw_dataframe.withColumn("file_name", input_file_name()) \
    .withColumn("value_map", get_data_map(col("value"))) \
    .withColumn("errors", validate(col("value_map"))) \
    .withColumn("is_valid", is_valid(col("errors"))) \
    .withColumn("current_date", F.current_date()) \
    .withColumn("year", F.year(col("current_date"))) \
    .withColumn("month", F.month(col("current_date"))) \
    .withColumn("day", F.dayofmonth(col("current_date")))

validated_df = validated_df.drop(validated_df.current_date)

valid_df = validated_df.where(validated_df.is_valid).select(validated_df.value_map, validated_df.year,
                                                            validated_df.month, validated_df.day)
invalid_df = validated_df.where(validated_df.is_valid == False)


@udf(returnType=StringType())
def get_as_string_column(data_map, index):
    return str(data_map[index])


@udf(returnType=IntegerType())
def get_as_integer_column(data_map, index):
    return int(data_map[index])


@udf(returnType=DoubleType())
def get_as_double_column(data_map, index):
    return float(data_map[index])


@udf(returnType=DateType())
def get_as_date_column(data_map, index, format_string):
    return datetime.strptime(data_map[index].strip(), format_string)


for column_schema in schema.columns:
    if column_schema.data_type == "StringType":
        valid_df = valid_df.withColumn(column_schema.name,
                                       get_as_string_column(col("value_map"), lit(column_schema.index)))
    elif column_schema.data_type == "IntegerType":
        valid_df = valid_df.withColumn(column_schema.name,
                                       get_as_integer_column(col("value_map"), lit(column_schema.index)))
    elif column_schema.data_type == "DoubleType":
        valid_df = valid_df.withColumn(column_schema.name,
                                       get_as_double_column(col("value_map"), lit(column_schema.index)))
    elif column_schema.data_type == "DateType":
        valid_df = valid_df.withColumn(column_schema.name,
                                       get_as_date_column(col("value_map"), lit(column_schema.index),
                                                          lit(column_schema.date_format)))

valid_df = valid_df.drop(valid_df.value_map)

if schema.save_valids_if_errors:
    valid_df.write.mode('append').partitionBy("year", "month", "day").parquet("s3://data-validation-poc/valid_dataset/")

invalid_df = invalid_df.drop(invalid_df.value_map) \
    .drop(invalid_df.is_valid)

invalid_df.write.mode('append').partitionBy("year", "month", "day").parquet("s3://data-validation-poc/invalid_dataset/")

number_of_errors = invalid_df.count()

message = {
    "raw_file_name": file_name,
    "dataset_name": schema.dataset_name,
    "total_number_of_records_in_file": raw_dataframe.count(),
    "total_number_of_valid_records": valid_df.count(),
    "total_number_of_invalid_records": number_of_errors,
    "validation_date_time": str(datetime.now()),
    "job_id": job_meta_args["JOB_ID"],
    "job_name": job_meta_args["JOB_NAME"],
    "job_run_id": job_meta_args["JOB_RUN_ID"],
    "total_processing_time": str(time.time() - start_time),
    "spark_application_id": spark.sparkContext.applicationId
}

write_summary(message)

if number_of_errors > 0:
    send_notification(message)

print("program completes ...........")
