import unittest
import sasi_model.tests
from sasi_model.tests.db_testcase import DBTestCase
from sasi_model.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
from sasi_model.model_runner import SASIModelRunner
from sasi_model.tests.generate_test_data import generate_data
import os


class ModelRunner_Test(DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)

    def test_ModelRunner(self):
        dataDir = generate_data()

        dao_opts = {
            'dao_type': 'sa',
            'session': self.session
        }
        runner = SASIModelRunner(dao_opts=dao_opts, dataDir=dataDir)
        runner.ingest_data()
        runner.process_ingested_data()

if __name__ == '__main__':
    unittest.main()
