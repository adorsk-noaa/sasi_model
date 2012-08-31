from sasi_model.ingestors.csv_ingestor import CSV_Ingestor
from sasi_model.models import (Substrate)
import os


class SASIModelRunner(object):

    def __init__(self, dao_opts={}, dataDir=None):
        self.dataDir = dataDir
        self.setup_dao(**dao_opts)

    def setup_dao(self, dao_type="sa", **kwargs):
        if dao_type == 'sa':
            from sasi_model.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
            self.dao = SASI_SqlAlchemyDAO(**kwargs)

    def ingest_data(self):
        data_sections = [
            {'id': 'substrates'},
        ]

        csv_sections = [
            {
                'id': 'substrates',
                'class': Substrate,
                'mappings': [
                    {'source': 'id', 'target': 'id'}
                ]
            }
        ]
        for section in csv_sections:
            csv_file = os.path.join(self.dataDir, section['id'], 'data',
                                    "%s.csv" % section['id'])
            ingestor = CSV_Ingestor(dao=self.dao, csv_file=csv_file,
                                    clazz=section['class'],
                                    mappings=section['mappings']
                                   )
            ingestor.ingest()

    def run_model(self):
        # Run model.
        # Return DAO.
        pass
