import unittest
from sasi_data.util import data_generators as dg
from sasi_data.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
from sasi_data.ingestors.sasi_ingestor import SASI_Ingestor
from sasi_model.sasi_model import SASI_Model
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import shutil
import platform
import sys
import time


class SASIModelTest(unittest.TestCase):

    @classmethod
    def setUpClass(clz):
        if platform.system() == 'Java':
            pass
        else:
            import pyspatialite
            sys.modules['pysqlite2'] = pyspatialite
            engine = create_engine('sqlite://')
            clz.con = engine.connect()
            clz.con.execute('SELECT InitSpatialMetadata()')
            Session = sessionmaker(bind=clz.con)
            clz.session = Session()

    def setUp(self):
        self.data_dir = dg.generate_data_dir()

    def tearDown(self):
        if self.data_dir:
            shutil.rmtree(self.data_dir)

    def test_sasi_model(self):
        dao = SASI_SqlAlchemyDAO(session=self.session)
        sasi_ingestor = SASI_Ingestor(dao=dao)
        sasi_ingestor.ingest(data_dir=self.data_dir)
        parameters = dao.query('__ModelParameters').one()
        cells = dao.query('__Cell').all()
        substrates = dao.query('__Substrate').all()
        features = dao.query('__Feature').all()
        gears = dao.query('__Gear').all()
        vas = dao.query('__VA').all()
        efforts = dao.query('__Effort').all()

        taus = {}
        omegas = {}
        for i in range(1,4):
            taus[i] = getattr(parameters, "t_%s" % i)
            omegas[i] = getattr(parameters, "w_%s" % i)

        s = time.time()
        m = SASI_Model(
            t0=parameters.time_start,
            tf=parameters.time_end,
            dt=parameters.time_step,
            taus=taus,
            omegas=omegas,
            dao=dao,
        )
        m.run(batch_size=100)
        e = time.time()
        print "t: ", e - s

if __name__ == '__main__' :
    unittest.main()

