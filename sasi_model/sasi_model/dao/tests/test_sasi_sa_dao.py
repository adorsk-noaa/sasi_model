import unittest
from sasi_model.tests.db_testcase import DBTestCase
from sasi_model.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
from sasi_model.models import (Cell, Habitat, Substrate, Feature, Gear,
                               Result, Effort)
from sqlalchemy import (Table, Column, ForeignKey, ForeignKeyConstraint, 
                        Integer, String, Float, create_engine, MetaData)
from sqlalchemy.orm import (scoped_session, sessionmaker)


class SASI_SqlAlchemyDAOTestCase(DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)
        self.dao = SASI_SqlAlchemyDAO(session=self.session)

if __name__ == '__main__':
    unittest.main()
