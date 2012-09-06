import unittest
from sasi_data.tests.generate_test_data import generate_data
from sasi_data.util.sa.tests.db_testcase import DBTestCase
from sasi_data.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
from sasi_data.ingestors.sasi_ingestor import SASI_Ingestor
from sasi_model.sasi_model import SASI_Model
import shutil

class SASIModelTest(DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)
        self.data_dir = generate_data()

    def tearDown(self):
        if self.data_dir:
            shutil.rmtree(self.data_dir)
        DBTestCase.tearDown(self)

    def test_sasi_model(self):
        dao = SASI_SqlAlchemyDAO(session=self.session)
        sasi_ingestor = SASI_Ingestor(dao=dao)
        sasi_ingestor.ingest(data_dir=self.data_dir)
        parameters = dao.query('{{ModelParameters}}').one()
        cells = dao.query('{{Cell}}').all()
        substrates = dao.query('{{Substrate}}').all()
        features = dao.query('{{Feature}}').all()
        gears = dao.query('{{Gear}}').all()
        vas = dao.query('{{VA}}').all()
        efforts = dao.query('{{Effort}}').all()

        taus = {}
        omegas = {}
        for i in range(1,4):
            taus[i] = getattr(parameters, "t_%s" % i)
            omegas[i] = getattr(parameters, "w_%s" % i)

        m = SASI_Model(
            t0=parameters.time_start,
            tf=parameters.time_end,
            dt=parameters.time_step,
            taus=taus,
            omegas=omegas,
            cells=cells,
            features=features,
            efforts=efforts,
            vas=vas,
            opts={'verbose': True}
        )
        m.run()
        print m.results

if __name__ == '__main__' :
    unittest.main()

