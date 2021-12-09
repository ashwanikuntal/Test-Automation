import datetime
import os
import pytest

from awsglue.context import GlueContext
from pyspark.sql import SparkSession

from automation import Transform, Driver


class Target(object):
    table_name = 'test_table'


class TestAutomation(object):
    jars_path = os.path.join(os.getcwd(), "jars", "*")
    spark = SparkSession \
        .builder \
        .appName("MSSQL to CSV") \
        .config("spark.driver.extraClassPath", jars_path) \
        .config("spark.executor.extraClassPath", jars_path) \
        .getOrCreate()

    sc = spark.sparkContext
    glue_context = GlueContext(sc)

    # def get_schema(self, parition_spec):
    #     self.schema_detail = [{"Name": "insert_Year", "Type": "int"}, {"Name": "insert_month", "Type": "int"},
    #                              {"Name": "insert_day", "Type": "int"}, {"Name": "insert_hours", "Type": "int"},
    #                              {"Name": "insert_mins", "Type": "int"}]
    #     return self.schema_detail
    #
    # def get_partition_location(self, partition_spec, partition):
    #     self.partition_location = 'dir/test_table/insert_sec=sec/insert_row=row/insert_x=x/insert_y=y/'
    #     return self.partition_location

    @pytest.fixture
    def source_schema(self):
        self.source_schema = [
            {
                'Name': 'insert_Year',
                'Type': 'int'
            },
            {
                'Name': 'insert_month',
                'Type': 'int'
            },
            {
                'Name': 'insert_day',
                'Type': 'int'
            },
            {
                'Name': 'insert_hours',
                'Type': 'int'
            },
            {
                'Name': 'insert_mins',
                'Type': 'int'
            }
        ]
        return self.source_schema

    @pytest.fixture
    def partition(self):
        self.partition = {'insert_sec': 'sec', 'insert_row': 'row', 'insert_x': 'x', 'insert_y': 'y'}
        return self.partition

    @pytest.fixture
    def target_location(self):
        self.target_location = 'dir/'
        return self.target_location

    @pytest.fixture
    def target(self):
        self.target = Target
        return self.target

    # @pytest.fixture
    # def schema(self):
    #     partition_spec = ['insert_sec', 'insert_row', 'insert_x', 'insert_y']
    #     self.schema = self.get_schema(partition_spec)
    #     return self.schema

    @pytest.fixture
    def target_s3_location(self):
        self.target_s3_location = 's3://my-artifact-store/destination/'
        return self.target_s3_location

    def test_add_time_partition_columns(self):
        df = self.sc.parallelize([
            (datetime.datetime(1984, 1, 1, 0, 0), 1, 638.55),
            (datetime.datetime(1984, 1, 1, 0, 0), 2, 638.55),
            (datetime.datetime(1984, 1, 1, 0, 0), 3, 638.55),
            (datetime.datetime(1984, 1, 1, 0, 0), 4, 638.55),
            (datetime.datetime(1984, 1, 1, 0, 0), 5, 638.55)
        ]).toDF()

        result = Transform.add_time_partition_columns(df, datetime.datetime(1984, 1, 1, 0, 0))
        assert result is not None

    def test_get_storage_descriptor_for_parquet(self, target_s3_location):
        format = 'PARQUET'
        table_name = 'test_table'
        schema = [{"Name": "insert_Year", "Type": "int"}, {"Name": "insert_month", "Type": "int"},
                                 {"Name": "insert_day", "Type": "int"}, {"Name": "insert_hours", "Type": "int"},
                                 {"Name": "insert_mins", "Type": "int"}]
        storage_desc_parquet = Driver.get_storage_descriptor(self, format, schema, table_name)
        assert storage_desc_parquet == {'Columns': [{'Name': 'insert_Year', 'Type': 'int'}, {'Name': 'insert_month', 'Type': 'int'}, {'Name': 'insert_day', 'Type': 'int'}, {'Name': 'insert_hours', 'Type': 'int'}, {'Name': 'insert_mins', 'Type': 'int'}], 'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat', 'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat', 'SerdeInfo': {'Parameters': {'serialization.format': '1'}, 'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'}, 'Location': 's3://my-artifact-store/destination/test_table/'}

    def test_get_storage_descriptor_for_csv(self, target_s3_location):
        format = 'CSV'
        table_name = 'test_table'
        schema = [{"Name": "insert_Year", "Type": "int"}, {"Name": "insert_month", "Type": "int"},
                  {"Name": "insert_day", "Type": "int"}, {"Name": "insert_hours", "Type": "int"},
                  {"Name": "insert_mins", "Type": "int"}]
        storage_desc_csv = Driver.get_storage_descriptor(self, format, schema, table_name)
        assert storage_desc_csv == {'Columns': [{'Name': 'insert_Year', 'Type': 'int'}, {'Name': 'insert_month', 'Type': 'int'}, {'Name': 'insert_day', 'Type': 'int'}, {'Name': 'insert_hours', 'Type': 'int'}, {'Name': 'insert_mins', 'Type': 'int'}], 'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat', 'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', 'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', 'Parameters': {'field.delim': ','}}, 'Location': 's3://my-artifact-store/destination/test_table/'}

    def test_get_storage_descriptor_for_json(self, target_s3_location):
        format = 'JSON'
        table_name = 'test_table'
        schema = [{"Name": "insert_Year", "Type": "int"}, {"Name": "insert_month", "Type": "int"},
                  {"Name": "insert_day", "Type": "int"}, {"Name": "insert_hours", "Type": "int"},
                  {"Name": "insert_mins", "Type": "int"}]
        storage_desc_json = Driver.get_storage_descriptor(self, format, schema, table_name)
        assert storage_desc_json == {'Columns': [{'Name': 'insert_Year', 'Type': 'int'}, {'Name': 'insert_month', 'Type': 'int'}, {'Name': 'insert_day', 'Type': 'int'}, {'Name': 'insert_hours', 'Type': 'int'}, {'Name': 'insert_mins', 'Type': 'int'}], 'InputFormat': '', 'OutputFormat': '', 'SerdeInfo': {}, 'Location': 's3://my-artifact-store/destination/test_table/'}

    # def test_get_partition_input(self, partition, schema):
    #     partition_spec = ['insert_sec', 'insert_row', 'insert_x', 'insert_y']
    #     result_partition_input = Transform.get_partition_input(self, partition_spec, partition)
    #     print(result_partition_input)
    #     return None

    def test_get_partition_location(self, partition, target_location, target):
        partition_spec = ['insert_sec', 'insert_row', 'insert_x', 'insert_y']
        result_location = Transform.get_partition_location(self, partition_spec, partition)
        assert result_location == 'dir/test_table/insert_sec=sec/insert_row=row/insert_x=x/insert_y=y/'

    def test_get_partition_schema(self, source_schema):
        partition_spec = ['insert_sec', 'insert_row', 'insert_x', 'insert_y']
        result_partition_schema = Transform.get_schema(self, partition_spec)
        assert result_partition_schema == [{'Name': 'insert_Year', 'Type': 'int'}, {'Name': 'insert_month', 'Type': 'int'}, {'Name': 'insert_day', 'Type': 'int'}, {'Name': 'insert_hours', 'Type': 'int'}, {'Name': 'insert_mins', 'Type': 'int'}]

    def test_get_schema(self, source_schema):
        partition_spec = ['insert_sec', 'insert_row', 'insert_x', 'insert_y']
        result_schema = Transform.get_schema(self, partition_spec)
        assert result_schema == [{"Name": "insert_Year", "Type": "int"}, {"Name": "insert_month", "Type": "int"},
                                 {"Name": "insert_day", "Type": "int"}, {"Name": "insert_hours", "Type": "int"},
                                 {"Name": "insert_mins", "Type": "int"}]

    def test_get_mappings(self, source_schema):
        result_list = Transform.get_mappings(self)
        assert result_list == [('insert_Year', 'int', 'insert_Year', 'int'), ('insert_month', 'int', 'insert_month', 'int'), ('insert_day', 'int', 'insert_day', 'int'), ('insert_hours', 'int', 'insert_hours', 'int'), ('insert_mins', 'int', 'insert_mins', 'int')]