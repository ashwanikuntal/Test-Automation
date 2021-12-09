import os
import unittest
import datetime
import automation


class TestFile(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_schema(self):
        partition_spec = ['insert_sec', 'insert_row', 'insert_x', 'insert_y']
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

        result_schema = automation.Transform.get_schema(self, partition_spec)
        self.assertTrue(result_schema == [{"Name": "insert_Year", "Type": "int"}, {"Name": "insert_month", "Type": "int"},
                                 {"Name": "insert_day", "Type": "int"}, {"Name": "insert_hours", "Type": "int"},
                                 {"Name": "insert_mins", "Type": "int"}])

    def test_get_mappings(self):
        # return_list = []
        # for i in self.source_schema:
        #     if i['Type'].find("decimal") != -1:
        #         return_list.append((i['Name'], i['Type'], i['Name'], "string"))
        #         value = "string"
        #     else:
        #         return_list.append((i['Name'], i['Type'], i['Name'], i['Type']))
        # return return_list
        result_list = automation.Transform.get_mappings(self)
        self.assertIsNotNone(result_list)
