import sasi_model.ingestors as ingestors 
import sasi_model.models as sasi_models
import sasi_model.util.gis as gis_util
import os
import csv


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
        for str_attr in ['projection']:
            mappings.append({'source': str_attr, 'target': str_attr})

        ingestor = ingestors.Parameters_Ingestor(csv_file=params_file, 
                                       mappings=mappings)
        self.model_parameters = ingestor.ingest()

        # Fishing efforts.
        effort_dir = os.path.join(self.dataDir, 'fishing_efforts')
        effort_model_file = os.path.join(effort_dir, 'model.csv')
        model_info_reader = csv.DictReader(open(effort_model_file, 'rb'))
        model_info = model_info_reader.next()
        self.effort_model_type = model_info.get('model_type')
        if self.effort_model_type == 'realized':
            csv_file = os.path.join(effort_dir, 'data', 'fishing_efforts.csv')
            mappings = []
            for int_attr in ['cell_id', 'time']:
                mappings.append({
                    'source': int_attr, 'target': int_attr, 
                    'processor': lambda value: int(value)
                })
            for float_attr in ['swept_area', 'hours_fished']:
                mappings.append({
                    'source': float_attr, 'target': float_attr, 
                    'processor': lambda value: float(value)
                })
            for str_attr in ['gear']:
                mappings.append({
                    'source': str_attr, 'target': str_attr, 
                })
            ingestor = ingestors.CSV_Ingestor(dao=self.dao, csv_file=csv_file,
                                    clazz=sasi_models.Effort,
                                    mappings=mappings
                                   )
            ingestor.ingest()

    def process_ingested_data(self):
        pass
        # Calculate cell compositions.
        # Populate fishing effort models.

    def calculate_cell_compositions(self):
        # Calculate areas for habitats.
        for habitat in self.dao.query('{{Habitat}}'):
            habitat.area = gis_util.get_area_from_wkb(
                habitat.geom.geom_wkb, 
                source_proj="EPSG:4326",
                target_proj=self.model_parameters.get('projection')
            )
            sa.session.add(hab)
            sa.session.commit()

        # Generate habitat compositions for cells.
        counter = 0
        for cell in cells_dao.all():
            if conf.conf['verbose']:
                if (counter % 100) == 0: print >> sys.stderr, "at cell # %s" % counter

            composition = {}

            # Get cell area.
            cell_area_entity = geo_func.area(func.geography(Cell.geom))
            cell.area = sa.session.query(cell_area_entity).filter(Cell.id == cell.id).one()[0]

            # Get habitats which intersect the cell.
            intersection_area_entity = geo_func.area(func.geography(geo_func.intersection(Habitat.geom, cell.geom)))
            results = sa.session.query(Habitat, intersection_area_entity).filter(Habitat.geom.intersects(cell.geom)).all()
            for result in results:
                hab = result[0]
                intersection_area = result[1]
                hab_key = (hab.substrate, hab.energy,)
                pct_area = intersection_area/cell.area
                composition[hab_key] = composition.get(hab_key, 0) + pct_area

            cell.habitat_composition = composition
            sa.session.add(cell)

            counter += 1

    def run_model(self):
        # Run model.
        # Return DAO.
        pass
