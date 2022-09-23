import csv
import os


import apache_beam as beam
from apache_beam.io import fileio

from define import (
    ReadingDetailsRecord,
    HeaderRecord
)

from parsers import (
    create_interval_record
)




def parse_file(tup):
    file_name = tup[0]
    lines = [x.strip('\r').split(",") for x in tup[1].split('\n')]

    header = lines[0]
    data = lines[1:]

    if header[0] == '100':
        header_obj = HeaderRecord(header,file_name)
    else:
        raise

    if header_obj.version_header == 'NEM12':
        parsed_records = create_interval_record(data, header_obj,file_name)
    else:
        pass
    return parsed_records

def normalizer(record):
    normalized = []
    file_name = record['file_name']
    header = record['HeaderRecord']
    if header.version_header == 'NEM12':
        nmi_details = record['IntervalMeterNMIDetailsRecord']
        for read_key in record['SingleReadingRecord']:
            read = record['SingleReadingRecord'][read_key]
            rd = ReadingDetailsRecord(file_name, header, nmi_details, read)
            normalized.append(rd)
    return normalized


def main():
    filenames = ["tests/" + f for f in os.listdir('tests/')]


    with beam.Pipeline() as pipeline:
        plants = (
          pipeline
          | "Create PCollection with required files" >> beam.Create(filenames)
          | "Match & select with required files" >> fileio.MatchAll()
          | "Read equired files" >> fileio.ReadMatches()
          | "Read entire file in one process" >> beam.Map(lambda file: (file.metadata.path, file.read_utf8()))
          | "Parse data" >> beam.Map(parse_file)
          ### RowGroup is defined as from one 200 or 250 to the next
          | "Flatten Rowgroups" >> beam.FlatMap(lambda x: x)
          | "Separate each individual read in a list" >> beam.Map(normalizer)
          | "Flatten the list" >> beam.FlatMap(lambda x: x)
          | "Serialize the class" >> beam.Map(lambda x: x.serialize())
          | beam.Map(print)
          )




if __name__ == '__main__':
    main()
