import unittest
from sasi_model.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
from sqlalchemy import (Table, Column, ForeignKey, ForeignKeyConstraint, 
                        Integer, String, Float, create_engine, MetaData)
from sqlalchemy.orm import (scoped_session, sessionmaker)


class SASI_SqlAlchemyDAOTestCase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://')
        self.connection = self.engine.connect()
        self.session = scoped_session(sessionmaker(bind=self.connection))

    def test_foo(self):
        print self.session


if __name__ == '__main__':
    unittest.main()



