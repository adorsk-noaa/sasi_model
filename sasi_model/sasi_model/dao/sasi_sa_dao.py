from sasi_model.models import (Cell, Habitat, Substrate, Feature, Gear,
                               Result, FishingEffort)
from sqlalchemy import (Table, Column, ForeignKey, ForeignKeyConstraint, 
                        Integer, String, Float, PickleType, create_engine, 
                        MetaData)
from sqlalchemy.orm import (mapper, relationship)
from geoalchemy import (GeometryExtensionColumn, MultiPolygon, GeometryDDL)


class SASI_SqlAlchemyDAO(object):

    def __init__(self, bind=None):
        self.bind = bind
        self.setup()
        pass

    def setUp(self):
        self.metadata = MetaData()
        self.schema = generateSchema()
        self.metadata.create_all()

    def generateSchema(self):
        schema = { 'sources': {} }

        # Cell.
        cell_table = Table('cell', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('type', String),
                           Column('type_id', Integer),
                           Column('area', Float),
                           Column('z', Float),
                           Column('habitat_composition', PickleType),
                           GeometryExtensionColumn('geom', MultiPolygon(2)),
                          )
        GeometryDDL(cell_table)
        mapper(Cell, cell_table, properties = {
            'geom': GeometryColumn(cell_table.c.geom),
        })

        # Habitat.
        habitat_table = Table('habitat', self.metadata,
                      Column('id', Integer, primary_key=True),
                      Column('substrate', String),
                      Column('energy', String),
                      Column('z', Float),
                      Column('area', Float),
                      GeometryExtensionColumn('geom', MultiPolygon(2)),
                     )
        GeometryDDL(habitat_table)
        mapper(Habitat, habitat_table, properties = {
            'geom': GeometryColumn(habitat_table.c.geom),
        })

        # Substrate.
        substrate_table = Table('substrate', self.metadata,
                                Column('id', String, primary_key=True),
                                Column('name', String)
                               )
        mapper(Substrate, table)

        # Feature.
        feature_table = Table('feature', self.metadata,
                              Column('id', String, primary_key=True),
                              Column('name', String),
                              Column('category', String)
                             )
        mapper(Feature, feature_table)

        # Gear.
        gear_table = Table('gear', self.metadata,
                           Column('id', String, primary_key=True),
                           Column('name', String),
                          )
        mapper(Gear, gear_table)

        # Effort.
        effort_table = Table('effort', self.metadata,
                           Column('id', String, primary_key=True),
                           Column('name', String),
                          )
        mapper(Effort, effort_table)

        # Result.
        result_table = Table('result', self.metadata,
                           Column('id', String, primary_key=True),
                           Column('name', String),
                          )
        mapper(Result, result_table)

        # Save sources in schema.
        for clazz in [Cell, Habitat, Substrate, Feature, Gear, Effort, 
                      Result]:
            schema['sources'][clazz.__name__] = clazz

        return schema

    def tearDown(self):
        # Remove db tables.
        pass
