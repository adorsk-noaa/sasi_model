import csv


class CSV_Ingestor(object):
    def __init__(self, csv_file=None, dao=None, clazz=None, mappings={}):
        self.dao = dao
        self.clazz = clazz
        self.mappings = mappings
        if isinstance(csv_file, str):
            csv_file = open(csv_file, 'rb')
        self.reader = csv.DictReader(csv_file)

    def ingest(self):
        for record in self.reader:
            obj = self.clazz()
            for mapping in self.mappings:
                raw_value = record.get(mapping['source'])
                processor = mapping.get('processor')
                if not processor:
                    processor = lambda value: value
                value = processor(raw_value)
                setattr(obj, mapping['target'], value)
            self.dao.save(obj)
