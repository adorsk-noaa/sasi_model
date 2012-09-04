import sasi_model.ingestors as ingestors 
import sasi_model.models as sasi_models
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
        
        # CSV data.
        csv_sections = [
            {
                'id': 'substrates',
                'class': sasi_models.Substrate,
                'mappings': [
                    {'source': 'id', 'target': 'id'}
                ]
            },
            {
                'id': 'features',
                'class': sasi_models.Feature,
                'mappings': [
                    {'source': 'id', 'target': 'id'},
                    {'source': 'category', 'target': 'category'}
                ]
            },
            {
                'id': 'gears',
                'class': sasi_models.Gear,
                'mappings': [
                    {'source': 'id', 'target': 'id'},
                ]
            },
            {
                'id': 'va',
                'class': sasi_models.VulnerabilityAssessment,
                'mappings': [
                    {'source': 'Gear ID', 'target': 'gear_id'},
                    {'source': 'Feature ID', 'target': 'feature_id'},
                    {'source': 'Substrate ID', 'target': 'substrate_id'},
                    {'source': 'Energy', 'target': 'energy'},
                ]
            }
        ]

        for section in csv_sections:
            csv_file = os.path.join(self.dataDir, section['id'], 'data',
                                    "%s.csv" % section['id'])
            ingestor = ingestors.CSV_Ingestor(dao=self.dao, csv_file=csv_file,
                                    clazz=section['class'],
                                    mappings=section['mappings']
                                   )
            ingestor.ingest()

        # Shapefile data.
        shp_sections = [
            {
                'id': 'habitats',
                'class': sasi_models.Habitat,
                'mappings': [
                    {'source': 'substrate', 'target': 'substrate'},
                    {'source': 'energy', 'target': 'energy'},
                    {'source': 'z', 'target': 'z', 
                     'processor': lambda value: -1.0 * float(value)},
                ]
            },
            {
                'id': 'grid',
                'class': sasi_models.Cell,
                'mappings': [
                    {'source': 'type', 'target': 'type'},
                    {'source': 'type_id', 'target': 'type_id'},
                ]
            }
        ]
        for section in shp_sections:
            shp_file = os.path.join(self.dataDir, section['id'], 'data',
                                    "%s.shp" % section['id'])
            ingestor = ingestors.Shapefile_Ingestor(dao=self.dao,
                shp_file=shp_file, clazz=section['class'], 
                mappings=section['mappings'] ) 
            ingestor.ingest()

        # Model parameters.
        params_file = os.path.join(self.dataDir, 'model_parameters', 'data',
                                   "model_parameters.csv")
        mappings = []
        for int_attr in ['time_start', 'time_end', 'time_step']:
            mappings.append({'source': int_attr, 'target': int_attr,
                             'processor': lambda value: int(value)})

        for i in range(1,4):
            for tw in ['t', 'w']:
                attr = "%s_%s" % (tw, i)
                mappings.append({'source': attr, 'target': attr,
                                 'processor': lambda value: float(value)})

        ingestor = ingestors.Parameters_Ingestor(csv_file=params_file, 
                                       mappings=mappings)
        model_parameters = ingestor.ingest()

        # Fishing efforts.

        """
        'fishing_efforts',
        """

        # Fishing effort data.

    def run_model(self):
        # Run model.
        # Return DAO.
        pass
