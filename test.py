import unittest
import app

import os, shutil
import uuid
import json

DEBUG = True

class TestNEM12Parsing(unittest.TestCase):

    def setUp(self):
        uid = str(uuid.uuid4())
        folder_name = "output/" + uid + "/"
        os.mkdir(folder_name)
        self.output_folder = folder_name
        input_folder = "tests/nem12/"
        output_folder = self.output_folder
        app.run(input_folder, output_folder)
        file_created = self.output_folder + "-00000-of-00001"
        self.data = [json.loads(j) for j in open(file_created).readlines()]

    def tearDown(self):
        if not DEBUG:
            shutil.rmtree(self.output_folder)


    def test_case1(self):
        r = [x for x in self.data if x['nmi_details']['nmi'] == 'NEM1203042' and x['single_read']['interval_date'] == '20040412' and x['single_read']['interval_value'] == "1"]
        expected = 13.35
        actual = float(r[0]['single_read']['interval_read'])
        self.assertEqual( actual, expected)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestNEM12Parsing('test_case1'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
