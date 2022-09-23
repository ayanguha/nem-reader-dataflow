
from datetime import datetime
from typing import Dict, List, NamedTuple, Optional
import json


class IntervalMeterNMIDetailsRecord:
    nmi: str
    nmi_configuration: str
    register_id: str
    nmi_suffix: str
    mdm_datastream_identifier: str
    meter_serial_number: str
    uom: str
    interval_length: int
    next_scheduled_read_date: Optional[datetime]

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


class HeaderRecord:
    version_header: str
    creation_date: Optional[datetime]
    from_participant: str
    to_participant: str
    file_name: str

    def __init__(self, header,file_name ):
        self.file_name = file_name
        self.version_header = header[1]
        self.creation_date = header[2]
        self.from_participant = header[3]
        self.to_participant = header[4]

class SingleReadingRecord:
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


class ReadingDetailsRecord:
    header: HeaderRecord
    nmi_details: IntervalMeterNMIDetailsRecord
    single_read: SingleReadingRecord

    def __init__(self,file_name, header_record, nmi_details_record, single_read):
        self.file_name = header_record.file_name
        self.version_header = header_record.version_header
        self.creation_date = header_record.creation_date
        self.from_participant = header_record.from_participant
        self.to_participant = header_record.to_participant

        self.nmi = nmi_details_record.nmi
        self.nmi_configuration = nmi_details_record.nmi_configuration
        self.register_id = nmi_details_record.register_id
        self.nmi_suffix = nmi_details_record.nmi_suffix
        self.mdm_datastream_identifier = nmi_details_record.mdm_datastream_identifier
        self.meter_serial_number = nmi_details_record.meter_serial_number
        self.uom = nmi_details_record.uom
        self.interval_length = nmi_details_record.interval_length
        self.next_scheduled_read_date = nmi_details_record.next_scheduled_read_date

        self.interval_date = single_read.interval_date
        self.interval_value = single_read.interval_value
        self.interval_read = single_read.interval_read
        self.quality_method = single_read.quality_method

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__)
