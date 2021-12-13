import overpy
import copy
from EDR.formatters import osm_to_covjson
from EDR.provider.base import BaseProvider, ProviderConnectionError, ProviderQueryError
import json


class OSMProvider(BaseProvider):

    def __init__(self, dataset, config):
        """initializer"""
        self.config = config

    def query(self, qtype, coords, time_range, z_value, params, outputFormat):
        
        api = overpy.Overpass()
        result = None
        output = {}
        query_str = ""

        if qtype == 'point':
            result = api.query("""
                way(around:100,"""+str(coords[1])+""",""" + str(coords[0])+""") ["highway"];
                (._;>;);
                out body;
                """)                     
            for param in params:
                query_str += 'way(around:100,'+str(coords[1])+',' + str(coords[0])+')[highway='+param+'];'

        elif qtype == 'polygon':
            yxcoords = ""
            for coord in coords:
                yxcoords += str(coord[1]) + " " + str(coord[0]) + " "

            for param in params:
                query_str += 'way(poly:"' + yxcoords.strip() + '")[highway='+param+'];'
            
        result = api.query('('+query_str+');(._;>;);out body;')

        output['type'] = "CoverageCollection"
        output['domainType'] = "Trajectory"

        output['coverages'] = [] 

        for way in result.ways:
            output['coverages'].append(osm_to_covjson.get_output(way, params, self.config))
        if len(output['coverages']) > 0:
            output['parameters'] = copy.deepcopy(output['coverages'][0]['parameters'])
        return json.dumps(output)


