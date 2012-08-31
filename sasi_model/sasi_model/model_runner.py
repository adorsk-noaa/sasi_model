class SASIModelRunner(object):

    def __init__(self, dao_opts={}):
        # Setup DAO.
        self.setup_dao(**dao_opts)

        # Ingest data.

    def setup_dao(self, dao_type="sa", **kwargs):
        if dao_type == 'sa':
            from sasi_model.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
            self.dao = SASI_SqlAlchemyDAO(**kwargs)

    def run_model(self):
        # Run model.
        # Return DAO.
        pass
