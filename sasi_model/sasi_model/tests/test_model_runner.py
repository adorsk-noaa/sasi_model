from sasi_model.tests.db_testcase import DBTestCase
from sasi_model.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
from sasi_model.model_runner import SASIModelRunner
import unittest


class ModelRunner_Test(DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)

    def test_ModelRunner(self):
        dao_opts = {
            'dao_type': 'sa',
            'session': self.session
        }
        runner = SASIModelRunner(dao_opts=dao_opts)
        print runner


if __name__ == '__main__':
    unittest.main()
