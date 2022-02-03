import json
import copy
import numpy
from numpy import ndarray
from netCDF4 import num2date
import regionmask
from siphon.catalog import TDSCatalog
from siphon.http_util import session_manager
from datetime import datetime, timedelta
from EDR.provider.metadata import MetadataProvider
from EDR.provider.base import BaseProvider, ProviderConnectionError, ProviderQueryError
from EDR.provider import InvalidProviderError
import EDR.isodatetime.data as timedata
from shapely.geometry import Point, Polygon


class ThreddsProvider(BaseProvider):


    def __init__(self, dataset, config):
        """initializer"""
        provider =  dataset.split("_")[0]
        self.DATA_SOURCE = config['datasets'][provider]['provider']['data_source']
        self.PROJECTION = config['datasets'][provider]['provider']['proj4']
        self.config = config
        self.dataset = dataset
        self.components = self.dataset.split('-')
        self.components[0] = self.components[0].split('_')[-1]        
        self.sub_detail = config['datasets'][provider]['provider']['sub_detail']
        self.datasets = config['datasets'][provider]['provider']['datasets']
        self.ds_name = config['datasets'][provider]['name']

    EXCLUDE_PARAMETERS = ['date','lon','lat','vertCoord', 'time','time1', 'height_above_ground', 'LatLon_Projection']

    pt_template = {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "domainType": "Point",
            "axes": {

            },
            "referencing": []
        },
        "parameters": {
        },
        "ranges": {
        }
    }

    axis_template = {"values": []}


    ref = {"GeographicCRS":
       {
           "coordinates": ["x", "y"],
           "system": {
               "type": "GeographicCRS",
               "id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
           }
       },
       "ProjectedCRS":
       {
           "coordinates": ["x", "y"],
           "system": {
               "type": "ProjectedCRS",
               "id": ""
           }
       },       
       "ISO8601":
       {
           "coordinates": ["t"],
           "system": {
               "type": "TemporalRS",
               "calendar": "Gregorian"
           }
       }
    }

    range_json = {
        "type": "NdArray",
        "dataType": "float",
        "axisNames": [],
        "shape": [],
        "values": []
    }



    def set_axis_vals(self, pt, axis_key, axis_vals):

        pt['domain']['axes'][axis_key] = copy.deepcopy(self.axis_template)

        if type(axis_vals) is list:
            pt['domain']['axes'][axis_key]['values'] = axis_vals
        else:
            pt['domain']['axes'][axis_key]['values'].append(axis_vals)

        return pt


    def set_parameter_vals(self, pt, data_param, variables):
        if self.config is not None:
            mp_ = MetadataProvider(self.config)
        for p in data_param:
            if (not (p in self.EXCLUDE_PARAMETERS)) and (not (p in self.components)):
                attr = variables[p]['attributes']
                pt['parameters'][p] = mp_.set_grib_detail(str(attr['Grib2_Parameter'][0]),str(attr['Grib2_Parameter'][1]),str(attr['Grib2_Parameter'][2]),attr['Grib2_Parameter_Name'],attr['units'])
        return pt

    def set_polygonparameter_vals(self, pt, data_param, variables):
        if self.config is not None:
            mp_ = MetadataProvider(self.config)
        for p in data_param:
            if (not (p in self.EXCLUDE_PARAMETERS)) and (not (p in self.components)):
                attr = variables[p]['attributes']
                pt['parameters'][p] = mp_.set_grib_detail(str(attr['Grib2_Parameter'][0]),str(attr['Grib2_Parameter'][1]),str(attr['Grib2_Parameter'][2]),attr['Grib2_Parameter_Name'],attr['units'])
        return pt
 
    def set_range_vals(self, pt, data, t_key):


        for p in data.keys():
            if (not (p in self.EXCLUDE_PARAMETERS)) and (not (p in self.components)):
                pt['ranges'][p] = copy.deepcopy(self.range_json)
                if len(self.components) > 3:
                    pt['ranges'][p]['axisNames'] = [t_key,'z']
                    pt['ranges'][p]['shape'] = [data[p].shape[0],1]
                else:
                    pt['ranges'][p]['axisNames'] = [t_key]
                    pt['ranges'][p]['shape'] = data[p].shape

                nda = numpy.ndarray(data[p].shape, buffer=data[p])

                pt['ranges'][p]['values'] = nda.flatten('C').tolist()
        return pt

    def set_polygonrange_vals(self, pt, data, t_key):

        for p in list(data.variables):
            if (not (p in self.EXCLUDE_PARAMETERS)) and (not (p in self.components)):
                pt['ranges'][p] = copy.deepcopy(self.range_json)

                if len(data.variables[p][:].shape) > 3:
                    pt['ranges'][p]['axisNames'] = [t_key, 'z', 'y','x']
                else:
                    pt['ranges'][p]['axisNames'] = [t_key,'y','x']
                pt['ranges'][p]['shape'] = data.variables[p][:].shape

                pt['ranges'][p]['values'] = data.variables[p][:].data.flatten('C').tolist()

        return pt

    def get_point(self, data, variables, t_data, x_key, y_key, t_key):

        pt = copy.deepcopy(self.pt_template)
        pt = self.set_axis_vals(pt, 'x', float(data[x_key][0]))
        pt = self.set_axis_vals(pt, 'y', float(data[y_key][0]))

        if t_key is not None:
            if len(t_data) > 1:
                pt['domain']['domainType'] = 'PointSeries'
            pt = self.set_axis_vals(pt, 't', t_data)

        
        pt['domain']['referencing'].append(self.ref['GeographicCRS'])


        if (t_key is not None):
            pt['domain']['referencing'].append(self.ref['ISO8601'])

        if len(self.components) > 3:
            pt = self.set_axis_vals(pt, 'z', data['vertCoord'][:].tolist()[0])            
            pt['domain']['referencing'].append(self.ref['ISO8601'])

        pt = self.set_parameter_vals(pt, data.keys(), variables)
        pt = self.set_range_vals(pt, data, 't')

        return pt

    def get_polygon(self, data, variables, t_data, t_key, coords):

        pt = copy.deepcopy(self.pt_template)
        pt = self.set_axis_vals(pt, 'x', (data.variables['lon'][:].data  - 360.0).tolist())
        pt = self.set_axis_vals(pt, 'y', data.variables['lat'][:].data.tolist())
        pt['domain']['domainType'] = 'Grid'
        zval = False
        if t_key is not None:
            pt = self.set_axis_vals(pt, 't', t_data)

        if len(self.components) > 3:
            pt = self.set_axis_vals(pt, 'z', data.variables[self.components[1]][:].data.tolist())
            zval = True

        pt['domain']['referencing'].append(self.ref['GeographicCRS'])
        if (t_key is not None):
            pt['domain']['referencing'].append(self.ref['ISO8601'])


        pt = self.set_parameter_vals(pt, list(data.variables), variables)
        
        pt = self.set_polygonrange_vals(pt, data, 't')

        nullset = self.set_null_values(coords, pt['domain']['axes'])

        dindex = 0
        t = 0
        z = 0
        if zval:
            while t < len(pt['domain']['axes']['t']['values']):
                while z < len(pt['domain']['axes']['z']['values']):
                    pt['ranges'], dindex = self.overwrite_withnull(pt['domain']['axes'], pt['ranges'], nullset, dindex)
                    z+=1
                t+=1
                z=0
        else:
            for t in pt['domain']['axes']['t']['values']:
                pt['ranges'], dindex = self.overwrite_withnull(pt['domain']['axes'], pt['ranges'], nullset, dindex)
        return pt

    def set_null_values(self, coords, axes):
        poly = Polygon(coords)
        nullset = []
        for y in axes['y']['values']:
            for x in axes['x']['values']:
                xy = Point(x, y)
                if not xy.within(poly): 
                    nullset.append(str(x)+'_'+str(y))        
        return nullset

    def overwrite_withnull(self, axes, ranges, nullset, dindex):
        for y in axes['y']['values']:
            for x in axes['x']['values']:
                if  str(x)+'_'+str(y) in nullset:
                    for pname in ranges:
                        ranges[pname]['values'][dindex] = 'null'              
                dindex += 1
        return ranges, dindex        


    def ds_to_json(self, input, xname, yname, variables):
        result = {}
        result['timesteps'] = {"data": [], "dims": [
            "t"], "attrs": {"units": "ISO8601"}}
        for t in input['date']:
            result['timesteps']['data'].append(t.isoformat().replace('+00:00','Z'))

        pt = self.get_point(input, variables,
                                result['timesteps']['data'], xname, yname, 'times')

        return json.dumps(pt)

    def initial_time(self,initial_time):
       initial_time=datetime.strptime(initial_time, "%Y-%m-%dT%H:%M:%SZ")
       return initial_time

    def ds_to_polygonjson(self, input, xname, yname, variables, coords):
        result = {}
        result['timesteps'] = {"data": [], "dims": [
            "t"], "attrs": {"units": "ISO8601"}}
        initial_time=input.variables[self.components[0]].units
        initial_time=initial_time.replace('Hour','')
        initial_time=initial_time.replace('since','')
        initial_time=initial_time.replace(' ','')
        ftime=list()
        time_delta=input.variables[self.components[0]][:].squeeze().data
        initial_time=self.initial_time(initial_time)
        for t in time_delta:
          t=timedelta(hours=t)
          iso_time=(initial_time+t).isoformat()+'Z' 
          result['timesteps']['data'].append(iso_time)
         

        pt = self.get_polygon(input, variables,
                                result['timesteps']['data'], 'times', coords)



        return json.dumps(pt)

    def query(self, dataset, qtype, coords, time_range, z_value, params, identifier, outputFormat):
        cat = TDSCatalog(self.DATA_SOURCE)

            
        for item in cat.catalog_refs:
            detail = TDSCatalog(cat.catalog_refs[item].href)
            for sub_detail in detail.catalog_refs:
                if sub_detail == self.sub_detail:
                    output = self.get_data(detail.catalog_refs[sub_detail].href, z_value, time_range, params, qtype, coords)



        return output.replace('NaN','null'), 'no_delete'

    def multipoint_query(self, coords, query, ncss):

        joutput = {}
        joutput['type'] = "CoverageCollection"
        joutput['domainType'] = "PointSeries"
        joutput['parameters'] = None
        joutput['coverages'] = []                                 
        for coord in coords:
            query = query.lonlat_point(coord[0], coord[1])
            data = ncss.get_data(query)
            variables = ncss.metadata.variables
            result = json.loads(self.ds_to_json(data, 'lon', 'lat', variables))
            if joutput['parameters'] is None:
                joutput['parameters'] = copy.deepcopy(result['parameters'])
                joutput['referencing'] = copy.deepcopy(result['domain']['referencing'])
            
            coverage = {}
            coverage['type'] = copy.deepcopy(result['type'])
            coverage['domain'] = {}
            coverage['domain']['type'] = copy.deepcopy(result['domain']['type'])
            coverage['domain']['axes'] = copy.deepcopy(result['domain']['axes'])
            coverage['ranges'] = copy.deepcopy(result['ranges'])
            joutput['coverages'].append(coverage)
        return json.dumps(joutput), 'no_delete'

    def polygon_query(self, coords, query):
        west = 1000
        east = -1000
        north = -100
        south = 100
        for coord in coords:
            if coord[0] < west:
                west = coord[0]
            if coord[0] > east:
                east = coord[0]
            if coord[1] < south:
                south = coord[1]
            if coord[1] > north:
                north = coord[1]
        return query.lonlat_box(west, east, south, north), 'no_delete'
            
    def process_coordinates(self, qtype, coords, query, ncss):
        output = ""
        if qtype == 'multipoint':
            output = self.multipoint_query(coords, query, ncss)
        else:
            if qtype == 'point':
                query = query.lonlat_point(coords[0], coords[1])
            elif qtype == 'polygon':
                query = self.polygon_query(coords, query)
                
            data = ncss.get_data(query)
            variables = ncss.metadata.variables
            output = {}
            if qtype == 'point':
                output = self.ds_to_json(data, 'lon', 'lat', variables)
            elif qtype == 'polygon':
                output = self.ds_to_polygonjson(data, 'lon', 'lat', variables, coords)
        return output
    
    def get_data(self, href, z_value, time_range, params, qtype, coords):
        output = ""
        thredds = TDSCatalog(href)
        index = -1
        if z_value:
           if '/' in z_value:
              z_value=z_value.split('/')[0]
        for ds in thredds.datasets:
            index += 1
            if ds.find(self.datasets) > -1:
                ncss, query = self.build_query(thredds, index, z_value, time_range, params)
                
                output = self.process_coordinates(qtype, coords, query, ncss)
        return output

    def build_query(self,thredds, index, z_value, time_range, params):
        ncss = thredds.datasets[index].subset()
        query = ncss.query()
        now = datetime.utcnow()
        if z_value is not None:
            query = query.vertical_level(z_value)
        if time_range is not None:
            query = query.time_range(time_range.get_start_date().get_datetime(), time_range.get_end_date().get_datetime())
        else:
            query = query.time_range(now, now + timedelta(days=7))

        if len(params) == 0:
            params = list(ncss.metadata.gridsets[self.dataset.replace(self.ds_name+"_","").replace("-"," ")]["grid"].keys())
        for var in params:
            query = query.variables(var)
        
        return ncss, query
