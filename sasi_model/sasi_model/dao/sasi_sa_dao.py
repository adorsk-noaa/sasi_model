from sasi_model.models import (Cell, Habitat, Substrate, Feature, Gear,
                               Result, Effort)
from sa_dao.orm_dao import ORM_DAO
from sqlalchemy import (Table, Column, ForeignKey, ForeignKeyConstraint, 
                        Integer, String, Float, PickleType, create_engine, 
                        MetaData)
from sqlalchemy.orm import (mapper, relationship)
from geoalchemy import (GeometryExtensionColumn, MultiPolygon, 
                        GeometryColumn, GeometryDDL)


class SASI_SqlAlchemyDAO(object):

    def __init__(self, session=None):
        self.session = session
        self.setUp()

    def setUp(self):
        self.metadata = MetaData()
        self.schema = self.generateSchema()
        self.metadata.create_all(bind=self.session.bind)
        self.sa_dao = ORM_DAO(session=self.session, schema=self.schema)

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
        mapper(Substrate, substrate_table)

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

        # Fishing Effort.
        effort_table = Table('effort', self.metadata,
                           Column('id', String, primary_key=True),
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

    def save(self, obj, commit=True):
        self.sa_dao.save(obj, commit=commit)

    def commit(self):
        self.sa_dao.commit()

    def query(self, query_def):
        return self.sa_dao.get_query(query_def)

