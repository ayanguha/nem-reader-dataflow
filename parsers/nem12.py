from datetime import datetime
from typing import Dict, List, NamedTuple, Optional
import json

from .define import (

    HeaderRecord,
    AbstractMeterNMIDetailsRecord
)

class IntervalMeterNMIDetailsRecord(AbstractMeterNMIDetailsRecord):

    interval_length: int

    def __init__(self, nmi_details ):
        self.nmi = nmi_details[1]
        self.nmi_configuration = nmi_details[2]
        self.register_id = nmi_details[3]
        self.nmi_suffix = nmi_details[4]
        self.mdm_datastream_identifier = nmi_details[5]
        self.meter_serial_number = nmi_details[6]
        self.uom = nmi_details[7]
        self.interval_length = int(nmi_details[8])
        self.next_scheduled_read_date = nmi_details[9]

class SingleIntervalReadRecord:
    interval_date: datetime
    interval_value: str
    interval_reading: str
    quality_method: str
    meter_serial_number: str
    reason_code: str
    reason_description: str

    def __init__(self,read):
        self.interval_date = read['read_date']
        self.interval_value = read['interval']
        self.interval_read = read['reading']
        self.quality_method = read['quality_method']


class IntervalReadingDetailsRecord:
    header: HeaderRecord
    nmi_details: IntervalMeterNMIDetailsRecord
    single_read: SingleIntervalReadRecord

    def __init__(self,header_record, nmi_details_record, single_read):
        self.header = header_record
        self.nmi_details = nmi_details_record
        self.single_read = single_read

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__)

def create_interval_record(data, header_obj, file_name):
    records = {}
    for row in data:
        if row[0] == '200':
            nmi_details = IntervalMeterNMIDetailsRecord(row)
            record_key = nmi_details.nmi + " ~ " + nmi_details.nmi_suffix
            interval_length = nmi_details.interval_length
            if record_key not in records:
                records[record_key] = {}
                records[record_key]['HeaderRecord'] = header_obj
                records[record_key]['file_name'] = file_name
                records[record_key]['IntervalMeterNMIDetailsRecord'] = nmi_details
                records[record_key]['SingleIntervalReadRecord'] = {}
        elif row[0] == '300':
            read_date = row[1]
            num_reads = int(24*60/interval_length)
            quality_method = row[num_reads + 2]
            for k in range(1,num_reads+1):
                individual_reading_key = read_date + '~' + str(k)
                read = {'read_date': read_date, 'interval': str(k), 'reading': row[k+1], 'quality_method': quality_method}

                records[record_key]['SingleIntervalReadRecord'][individual_reading_key] = SingleIntervalReadRecord(read)

        elif row[0] == '400':
            start_pos = int(row[1])
            end_pos = int(row[2])
            quality_method = row[3]
            for k in range(start_pos, end_pos + 1):
                individual_reading_key = read_date + '~' + str(k)
                records[record_key]['SingleIntervalReadRecord'][individual_reading_key].quality_method = quality_method
    return list(records.values())
