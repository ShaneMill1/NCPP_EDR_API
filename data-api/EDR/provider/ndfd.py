from EDR.cache_logic import ndfd_data_cache
import xarray as xr
import glob
from os import listdir, path, sep
import json
import regionmask
from shapely.geometry import Point, Polygon
from EDR.templates import edrpoint, edrpolygon
import pyproj
import numpy as np
import pandas as pd
import copy
import metpy
import metpy.calc as mpcalc
from EDR.provider.base import BaseProvider, ProviderConnectionError, ProviderQueryError
from EDR.provider import InvalidProviderError

class NDFDSurfaceProvider(BaseProvider):

    def __init__(self, dataset, config):
        """initializer"""
        parts = dataset.split("_")
        self.DATASET_FOLDER = config['datasets'][parts[0]]['provider']['data_source']
        self.NDFD_PROJECTION = config['datasets'][parts[0]]['provider']['proj4']
        self.WGS84 = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'


        self.region = "AR." + parts[0]
        self.period = "VP." + parts[1]

        with open(self.DATASET_FOLDER+'/coord_info.json') as json_file:
            data_info = json.load(json_file)
            self.latest_folder = data_info['folder']
            self.data_files = []
            for c in data_info[self.region][self.period]:
                p_list = list(c.keys())
                for param in p_list:
                    if parts[2] == param.split('_')[-1]:
                        self.data_files.append(c[param]['file'])
        self.config = config
        self.dataset = dataset

    def add_param(self, dname):
        pname = dname.split(".")[1]
        ds_zarr = xr.open_zarr(
            self.DATASET_FOLDER +'/'+ self.latest_folder + '/' + self.region + '/' + self.period + '/zarr/' + 'ds.'+pname+'.zarr')
        for index in dict(ds_zarr.data_vars):
            ds_zarr[index].attrs['short_name'] = pname
        return ds_zarr


    def load_data(self):
        global ndfd_data_cache
        flist = []

        if (ndfd_data_cache is None) or (ndfd_data_cache == {}):
            ndfd_data_cache = {}
        
        if (self.dataset not in ndfd_data_cache) or (not ndfd_data_cache[self.dataset]['folder'] == self.latest_folder):

            ndfd_data_cache[self.dataset] = {}
            ndfd_data_cache[self.dataset]['folder'] = self.latest_folder
            for p in self.data_files:
                p_data = self.add_param(p)
                flist.append(p_data)                        

            ndfd_data_cache[self.dataset]['data'] = xr.merge(flist)


        return ndfd_data_cache[self.dataset]['data']

    def ds_to_json(self, input, xname, yname, pnames):
        result = {}
        result['timesteps'] = {"data": [], "dims": [
            "t"], "attrs": {"units": "ISO8601"}}
        for t in input['coords']['forecast_time0']['data']:
            result['timesteps']['data'].append(t.isoformat() + 'Z')

        # Convert coordinates to Lat Lon
        if (xname.find('lon') == -1):
            x,y = pyproj.transform(pyproj.Proj(self.NDFD_PROJECTION),pyproj.Proj('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'), float(input['coords'][xname]['data']), float(input['coords'][yname]['data']))
            input['coords'][yname]['data'] = y
            input['coords'][xname]['data'] = x
        else:
            input['coords'][yname]['data'] = input['coords'][yname]['data'] - 360

        pt = edrpoint.get_point(input['coords'], input['data_vars'],
                                result['timesteps']['data'], xname, yname, 'times', pnames, self.config, self.dataset)

        return pt

    def ds_to_polygonjson(self, input, xname, yname, params):
        result={}
        result['timesteps'] = {"data":[], "dims":["t"],"attrs":{"units":"ISO8601"}}
        for t in input['coords']['forecast_time0']['data']:
            result['timesteps']['data'].append(t.isoformat() + 'Z')


        pt = edrpolygon.get_polygon(input['coords'], input['data_vars'],
                            result['timesteps']['data'], xname, yname, 'times', params, self.config, self.dataset, self.config['server']['url'] + "/metadata/proj4/" + self.region.split(".")[1])

        return pt


    def query(self, dataset, qtype, coords, time_range, z_value, params, identifier, outputFormat):

        output = None

        ds = self.load_data()
        if time_range is not None:
            start=str(time_range.get_start_date())
            end=str(time_range.get_end_date())
            start = pd.Timestamp(start)
            end = pd.Timestamp(end)
            ds = ds.sel(forecast_time0=slice(start, end))

        if qtype == 'point':

            if 'lat_0' in list(ds.dims):
                output = self.ds_to_json(
                    ds.sel(lon_0=coords[0]+360, lat_0=coords[1], method='nearest').to_dict().sortby('forecast_time0'), 'lon_0', 'lat_0', params)

            elif 'xgrid_0' in list(ds.dims):      
                x,y = pyproj.transform(pyproj.Proj(self.WGS84), pyproj.Proj(self.NDFD_PROJECTION), coords[0], coords[1])
                output = self.ds_to_json(
                        ds.sel(xgrid_0=x, ygrid_0=y, method='nearest').sortby('forecast_time0').to_dict(),'xgrid_0','ygrid_0', params)

        elif qtype == 'multipoint':
            output = {}
            output['type'] = "CoverageCollection"
            output['domainType'] = "PointSeries"
            output['parameters'] = None
            output['coverages'] = [] 
            for coord in coords:
                result = {}
                if 'lat_0' in list(ds.dims):
                    result = self.ds_to_json(
                        ds.sel(lon_0=coords[0]+360, lat_0=coords[1], method='nearest').to_dict().sortby('forecast_time0'), 'lon_0', 'lat_0', params)

                elif 'xgrid_0' in list(ds.dims):      
                    x,y = pyproj.transform(pyproj.Proj(self.WGS84), pyproj.Proj(self.NDFD_PROJECTION), coords[0], coords[1])
                    result = self.ds_to_json(
                            ds.sel(xgrid_0=x, ygrid_0=y, method='nearest').sortby('forecast_time0').to_dict(),'xgrid_0','ygrid_0', params)
                
                if output['parameters'] is None:
                    output['parameters'] = copy.deepcopy(result['parameters'])
                    output['referencing'] = copy.deepcopy(result['domain']['referencing'])
                
                coverage = {}
                coverage['type'] = copy.deepcopy(result['type'])
                coverage['domain'] = {}
                coverage['domain']['type'] = copy.deepcopy(result['domain']['type'])
                coverage['domain']['axes'] = copy.deepcopy(result['domain']['axes'])
                coverage['ranges'] = copy.deepcopy(result['ranges'])
                output['coverages'].append(coverage)

        elif qtype == 'polygon':
            xycoords = []
            if 'xgrid_0' in list(ds.dims):
                xname = 'xgrid_0'
                yname = 'ygrid_0'
            else:
                xname = 'lon_0'
                yname = 'lat_0'
            for coord in coords:
                if  xname == 'xgrid_0':
                    x,y = pyproj.transform(pyproj.Proj(self.WGS84), pyproj.Proj(self.NDFD_PROJECTION), coord[0], coord[1])
                else:
                    x,y = coords[0]+360, coords[1]
                xycoords.append([x,y])


            poly = Polygon(xycoords)
            pb = poly.bounds
               
            cmask = regionmask.Regions_cls(['Selected_area'],[0],['selected_area'],['sa'],[[[pb[0],pb[1]],[pb[0],pb[3]],[pb[2],pb[3]],[pb[2],pb[1]],[pb[0],pb[1]]]])

            samask = cmask.mask(ds, lon_name=xname, lat_name=yname)
            
            if len(params) == 0:
                output = self.ds_to_polygonjson(ds.where(samask == cmask.map_keys('sa'), drop=True).sortby('forecast_time0').to_dict(), xname, yname, params)
            else:
                output = self.ds_to_polygonjson(ds[params].where(samask == cmask.map_keys('sa'), drop=True).sortby('forecast_time0').to_dict(), xname, yname, params)
        
            nullset = []
            for y in output['domain']['axes']['y']['values']:
                for x in output['domain']['axes']['x']['values']:
                    xy = Point(x, y)
                    if not xy.within(poly): 
                        nullset.append(str(x)+'_'+str(y))

            dindex = 0
            for t in output['domain']['axes']['t']['values']:
                for y in output['domain']['axes']['y']['values']:
                    for x in output['domain']['axes']['x']['values']:
                        if  str(x)+'_'+str(y) in nullset:
                            for pname in output['ranges']:
                                output['ranges'][pname]['values'][dindex] = 'null'              
                        dindex += 1

        return json.dumps(output).replace('NaN','null')
