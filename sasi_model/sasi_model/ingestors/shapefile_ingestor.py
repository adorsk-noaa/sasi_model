import shapefile


class Shapefile_Ingestor(object):

    shapetypes = {
        0 : 'NULL',
        1 : 'POINT',
        3 : 'POLYLINE',
        5 : 'POLYGON',
        8 : 'MULTIPOINT',
        11 : 'POINTZ',
        13 : 'POLYLINEZ',
        15 : 'POLYGONZ',
        18 : 'MULTIPOINTZ',
        21 : 'POINTM',
        23 : 'POLYLINEM',
        25 : 'POLYGONM',
        28 : 'MULTIPOINTM',
        31 : 'MULTIPATCH',
    }

    wkt_shapetypes = {
        'POINT' : 'MULTIPOINT',
        'POLYLINE' : 'MULTILINESTRING',
        'POLYGON' : 'MULTIPOLYGON'
    }

    def __init__(self, shp_file=None, dao=None, clazz=None, mappings={},
                 geom_attr='geom'):
        self.dao = dao
        self.clazz = clazz
        self.mappings = mappings
        self.reader = shapefile.Reader(shp_file)
        self.geom_attr = geom_attr

    def ingest(self):
        fields = self.reader.fields
        field_indices = dict((fields[i][0], i-1) for i in range(len(fields)))
        shapetype = self.shapetypes[self.reader.shapeType]
        wkt_shapetype = self.wkt_shapetypes[shapetype]
        for record in self.reader.shapeRecords():
            obj = self.clazz()

            for mapping in self.mappings:
                field_idx = field_indices.get(mapping.get('source'))
                if field_idx == None:
                    continue
                raw_value = record.record[field_idx]
                processor = mapping.get('processor')
                if not processor:
                    processor = lambda value: value
                value = processor(raw_value)
                setattr(obj, mapping['target'], value)

            parts = []
            for i in range(len(record.shape.parts)):
                start_idx = 0
                if (i+1) > len(parts):
                    end_idx = -1
                else:
                    end_idx = parts[i+1]
                parts.append(record.shape.points[start_idx:end_idx])
            wkt_parts = []
            for part in parts:
                wkt_points = ', '.join(["%s %s" % (point[0], point[1]) 
                              for point in part])
                wkt_part = "(%s)" % (wkt_points)
                wkt_parts.append(wkt_part)
            wkt_parts = ','.join(wkt_parts)
            wkt_geom = "%s((%s))" % (wkt_shapetype, wkt_parts)
            if self.geom_attr and hasattr(obj, self.geom_attr):
                setattr(obj, self.geom_attr, wkt_geom)

            self.dao.save(obj)
