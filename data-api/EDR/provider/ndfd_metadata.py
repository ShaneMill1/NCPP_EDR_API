import json
from datetime import datetime
import copy
from EDR import util

class NDFDMetadataProvider():

    def __init__(self, server, mp):
        """
        Initialize object

        """
        self.server = server
        self.ndfd_config = '/data/ndfd/coord_info.json'
        #self.ndfd_config = '/mnt/data-api/ndfd/coord_info.json'
        self.mp_ = mp

    def get_metadata(self, c, collection):

        if self.ndfd_config == None:
            with open(collection['provider']['data_source']+'coord_info.json') as json_file:
                self.ndfd_config = json.load(json_file)
        
        if self.ndfd_config != None:
            try:
               with open(self.ndfd_config) as json_file:
                  self.ndfd_config = json.load(json_file)
            except:
               self.ndfd_config='/mnt/data-api/ndfd/coord_info.json'
               with open(self.ndfd_config) as json_file:
                  self.ndfd_config = json.load(json_file)       
 
        for region in self.ndfd_config:
            if (not region == 'folder') and (region.split('.')[1] == c):
                collections = self.generate_metadata(region, collection)
        return collections


    def generate_metadata(self, region, collection):
 
        collections = [] 
        for period in self.ndfd_config[region]:
            for c in self.ndfd_config[region][period]:
                params = list(c.keys())

                try:                            
                    cname = region.replace(
                        "AR.", "") + "_" + period.replace("VP.", "") + "_" + params[0].split('_')[-1]

                    description = self.description_header(cname, collection, c[params[0]])

                    description["parameters"] = self.parameter_description(c, params, description["extent"])

                    description["crs"] = [{"id":"NATIVE","wkt":util.proj2wkt(collection["provider"]['proj4'])}]
                    
                    description["polygon-query-options"] = {}
                    description["polygon-query-options"]["interpolation-x"] = [
                        "nearest_neighbour"]
                    description["polygon-query-options"]["interpolation-y"] = [
                        "nearest_neighbour"]
                    description["point-query-options"] = {}
                    description["point-query-options"]["interpolation"] = [
                        "nearest_neighbour"]

                    description["f"] = [
                        "CoverageJSON"]

                    collections.append(
                        description)
                except KeyError as ke:
                    print(cname + ' :' + str(ke))
        return collections

    def get_times(self, coll):
        times = []
        if 'steps' in coll:
            for t in coll['steps']:
                if not (coll['steps'][0] is None):
                    try:
                        times.append(datetime.fromtimestamp(
                            t/1e9).isoformat()+'Z')
                    except TypeError as te:
                        print('missing timestep ' + str(te))
            times.sort()
        return times
    
    def get_extent(self, coll):
        min_lon = 200
        min_lat = 100
        max_lon = -200
        max_lat = -100

        if 'corners' in coll['lon']:
            min_lon = min(
                coll['lon']['corners'])
            min_lat = min(
                coll['lat']['corners'])
            max_lon = max(
                coll['lon']['corners'])
            max_lat = max(
                coll['lat']['corners'])
        else:
            min_lon = coll['lon']['Lo1']
            min_lat = coll['lat']['La1']
            max_lon = coll['lon']['Lo2']
            max_lat = coll['lat']['La2']
        return  min_lon, min_lat, max_lon, max_lat      

    def description_header(self, cname, collection, c_params):

        times = self.get_times(c_params)
        
        min_lon, min_lat, max_lon, max_lat = self.get_extent(c_params)
        
        cn_parts = cname.split('_')
        description = {"id": cname,
                        "title": cn_parts[0].upper() + ' days ' + cn_parts[1] + ' ' + collection['shortnames'][cn_parts[2]] + ' values',
                        "description": copy.deepcopy(collection['description']),
                        "extent": {
                            "horizontal": util.horizontaldef(["longitude","latutude"],["x","y"],[min_lon, min_lat, max_lon, max_lat]),
                            "temporal": util.coorddef(["time"],["time"],times)
                        },
                        "grid-metdata" : {},                                                       
                        "links": [util.createquerylinks(self.server + '/collections/' + cname + '/latest/position','self','position','Position query for latest ' + cname),util.createquerylinks(self.server + '/collections/' + cname + '/latest/area','self','area','Area query for latest ' + cname)]

                        }
        return description
    
    def parameter_description(self, c, params, extent):
        p_def = {}
        for p in params:
            p_def[c[p]['long_name']] = (self.mp_.set_grib_detail("grib2", "codeflag", "4.2-"+str(c[p]['grib2_code'][1])+"-"+str(
                c[p]['grib2_code'][2])+"-"+str(c[p]['grib2_code'][3]), c[p]['long_name'].replace('_',' '), c[p]['units']))
            p_def[c[p]['long_name']]['extent'] = extent
        return p_def        
