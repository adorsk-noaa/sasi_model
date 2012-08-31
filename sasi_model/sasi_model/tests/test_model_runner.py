from sasi_model.tests.db_testcase import DBTestCase
from sasi_model.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
import unittest


class ModelRunner_Test(DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)
        self.dao = SASI_SqlAlchemyDAO(session=self.session)

    def test_foo(self):
        print "yo"

if __name__ == '__main__':
    unittest.main()
