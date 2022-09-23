from datetime import datetime
from typing import Dict, List, NamedTuple, Optional
import json

from .define import (

    HeaderRecord,
    AbstractMeterNMIDetailsRecord
)

class BasicMeterNMIDetailsRecord(AbstractMeterNMIDetailsRecord):

    direction_indicator: str

    def __init__(self, nmi_details ):
        self.nmi = nmi_details[1]
        self.nmi_configuration = nmi_details[2]
        self.register_id = nmi_details[3]
        self.nmi_suffix = nmi_details[4]
        self.mdm_datastream_identifier = nmi_details[5]
        self.meter_serial_number = nmi_details[6]
        self.direction_indicator = nmi_details[7]
        self.uom = nmi_details[19]
        self.next_scheduled_read_date = nmi_details[20]

class SingleBasicReadRecord:
    previous_register_read: str
    previous_register_read_datetime: datetime
    previous_quality_method: str
    previous_reason_code: int
    previous_reason_description: str
    current_register_read: str
    current_register_read_datetime: datetime
    current_quality_method: str
    current_reason_code: int
    current_reason_description: str
    quantity: float
    update_datetime: datetime
    msats_load_datetime: datetime


    def __init__(self,row):
        self.previous_register_read = row[8]
        self.previous_register_read_datetime = row[9]
        self.previous_quality_method = row[10]
        self.previous_reason_code = row[11]
        self.previous_reason_description = row[12]
        self.current_register_read_datetime = row[14]
        self.current_quality_method = row[15]
        self.current_reason_code = row[16]
        self.current_reason_description = row[17]
        self.quantity = row[18]
        self.update_datetime = row[21]
        self.msats_load_datetime = row[22]


class BasicReadingDetailsRecord:
    header: HeaderRecord
    nmi_details: BasicMeterNMIDetailsRecord
    single_read: SingleBasicReadRecord

    def __init__(self,file_name, header_record, nmi_details_record, single_read):
        self.file_name = header_record.file_name
        self.header = header_record
        self.nmi_details = nmi_details_record
        self.single_read = single_read

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__)

def create_basic_record(data, header_obj, file_name):
    records = []
    for row in data:
        if row[0] == '250':
            nmi_details = BasicMeterNMIDetailsRecord(row)
            record = {}
            record['HeaderRecord'] = header_obj
            record['file_name'] = file_name
            record['BasicMeterNMIDetailsRecord'] = nmi_details
            record['SingleBasicReadRecord'] = SingleBasicReadRecord(row)
            records.append(record)

    return records
