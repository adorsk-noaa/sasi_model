import unittest
from shapely import wkt, wkb
import shapely.geometry
import sasi_model.util.gis as gis_util


class GISUtilTestCase(unittest.TestCase):
    def test_get_area_from_wkb(self):
        p1 = "+init=epsg:4326"
        p2 = "+proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"
        wkt_strs = {
            'MULTIPOLYGON': """MULTIPOLYGON(((-69.3332413066 42.089651444, -69.3336502149 41.9745847041, -69.3468402668 41.9560938039, -69.3947754295 41.9666518146, -69.4995605972 42.0797279043, -69.3785989571 42.1243718051, -69.3332413066 42.089651444)))""",
            'POLYGON': """POLYGON ((35 10, 10 20, 15 40, 45 45, 35 10),(20 30, 35 35, 30 20, 20 30))""",
        }
        for shape_type, wkt_str in wkt_strs.items():
            shape = wkt.loads(wkt_str)
            wkb_str = wkb.dumps(shape)
            area = gis_util.get_area_from_wkb(wkb_str, p1, p2)
            print "a is: ", area

        co = {"type": "Polygon", 
              "coordinates": [[
                  (-102.05, 41.0), (-102.05, 37.0), 
                  (-109.05, 37.0), (-109.05, 41.0)
              ]]
             }
        co_shape = shapely.geometry.shape(co)
        co_area = gis_util.get_area_from_wkb(wkb.dumps(co_shape), p1, p2)
        print co_area

if __name__ == '__main__':
    unittest.main()
