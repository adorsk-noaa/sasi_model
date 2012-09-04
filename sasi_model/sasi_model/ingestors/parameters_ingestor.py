import csv


class Parameters_Ingestor(object):
    def __init__(self, csv_file=None, mappings={}):
        self.mappings = mappings
        if isinstance(csv_file, str):
            csv_file = open(csv_file, 'rb')
        self.reader = csv.DictReader(csv_file)

    def ingest(self):
        parameters = {}
        for record in self.reader:
            for mapping in self.mappings:
                raw_value = record.get(mapping['source'])
                processor = mapping.get('processor')
                if not processor:
                    processor = lambda value: value
                value = processor(raw_value)
                parameters[mapping['target']] = value
        return parameters
