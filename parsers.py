
from define import (
    ReadingDetailsRecord,
    HeaderRecord,
    IntervalMeterNMIDetailsRecord,
    SingleReadingRecord
)

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
                records[record_key]['SingleReadingRecord'] = {}
        elif row[0] == '300':
            read_date = row[1]
            num_reads = int(24*60/interval_length)
            quality_method = row[num_reads + 2]
            for k in range(1,num_reads+1):
                individual_reading_key = read_date + '~' + str(k)
                read = {'read_date': read_date, 'interval': str(k), 'reading': row[k+1], 'quality_method': quality_method}

                records[record_key]['SingleReadingRecord'][individual_reading_key] = SingleReadingRecord(read)

        elif row[0] == '400':
            start_pos = int(row[1])
            end_pos = int(row[2])
            quality_method = row[3]
            for k in range(start_pos, end_pos + 1):
                individual_reading_key = read_date + '~' + str(k)
                records[record_key]['SingleReadingRecord'][individual_reading_key].quality_method = quality_method

    return list(records.values())
