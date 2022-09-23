
from datetime import datetime
from typing import Dict, List, NamedTuple, Optional
import json

class AbstractMeterNMIDetailsRecord:
    nmi: str
    nmi_configuration: str
    register_id: str
    nmi_suffix: str
    mdm_datastream_identifier: str
    meter_serial_number: str
    uom: str
    next_scheduled_read_date: Optional[datetime]



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
