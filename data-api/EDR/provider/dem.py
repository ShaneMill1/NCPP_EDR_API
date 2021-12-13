
import copy
import shapely.wkt
from EDR.provider.tileset import TILESET 
from EDR.formatters import dem_to_covjson
from EDR.provider.base import BaseProvider, ProviderConnectionError, ProviderQueryError
import json

class DEMProvider(BaseProvider):


    def __init__(self, dataset, config):
        """initializer"""
        self.config = config

    def query(self, qtype, coords, time_range, z_value, params, outputFormat):
        tileset_ = TILESET()         
        output = {}

        if qtype == 'point':
            elev = tileset_.get_elevation(coords[1], coords[0])
            output = dem_to_covjson.get_point(coords[0], coords[1], elev)

        elif qtype == 'polygon':
            polygon = "POLYGON(("
            delim = ""
            for coord in coords:
                polygon += delim + str(coord[0]) + " " + str(coord[1]) 
                delim = ", "
            polygon += delim +  str(coords[0][0]) + " " + str(coords[0][1]) +"))"   
            wkt = shapely.wkt.loads(polygon)

            lngs, lats, elevs  = tileset_.get_elevations(wkt)
            output = dem_to_covjson.get_polygon(lngs, lats, elevs)

        return json.dumps(output), 'no_delete'


