from datetime import datetime
import xarray as xr
import geopandas as gpd
import copy
from EDR.provider import concatenate_covjson
from datacube.utils.cog import write_cog
import glob
import json
from shapely.geometry import Point, Polygon, LineString, MultiLineString
from shapely.ops import transform
from EDR.templates import edrpoint, edrpolygon
import pyproj
import numpy as np
import os
from EDR.provider.base import BaseProvider, ProviderConnectionError, ProviderQueryError
from EDR.provider import InvalidProviderError
from EDR.formatters import format_output as fo
import yaml
import dask
import rioxarray
from EDR.provider import convert_to_grib
import flask
import urllib.request, json
import uuid as ud
import re
import cfgrib
import zipfile
import pathlib
import fsspec
from scipy import spatial


class s3_zarr_Provider(BaseProvider):

    def __init__(self,dataset,config):
        """initializer"""
        self.zarr_store = config['datasets'][dataset]['provider']['data_source']
        self.latkey='lat'
        self.lonkey='lon'
        self.fkey='time'
        self.uuid=str(ud.uuid4().hex)
        self.DATASET_FOLDER = config['datasets'][dataset]['provider']['output_location']
        self.dir_root=self.DATASET_FOLDER.replace('/collections','')
    
    def load_data(self,zarr_loc):
       fs = fsspec.filesystem('s3', anon=True)
       ds = xr.open_dataset(fs.get_mapper(zarr_loc), engine='zarr', chunks={},backend_kwargs=dict(consolidated=True))
       return ds
    
    
    def find_initial_time(self,initial_time):
       initial_time=initial_time.replace('(','')
       initial_time=initial_time.replace(')','')
       initial_time=initial_time.replace('/','-')
       month=initial_time[0:2]
       day=initial_time[3:5]
       year=initial_time[6:10]
       time=initial_time[11:16]
       initial_time=year+'-'+month+'-'+day+' '+time+':00'
       initial_time=datetime.strptime(initial_time, "%Y-%m-%d %H:%M:%S")
       return initial_time
 

    def pt_to_covjson(self,query_dict,coords,qtype):
       output=query_dict
       output['domain']={}
       output['type']="Coverage"
       output['domain']['type']="Domain"
       output['parameters'] = output.pop('data_vars')
       output['domain']['axes']={}
       if qtype=='point':
          output['domain']['domainType']="PointSeries"
          output['domain']['axes']['x']={}
          output['domain']['axes']['y']={}
          output['domain']['axes']['x']['values']=[output['coords']['longitude']['data']]
          output['domain']['axes']['y']['values']=[output['coords']['latitude']['data']]
       del output['coords']['latitude']['data']
       del output['coords']['longitude']['data']
       output['ranges']={}
       #find dim with "lv" and create the "z" dimension
       for key in output['coords']:
          if 'lv' in key:
             z_key=key
             output['domain']['axes']['z']={}
             if type(output['coords'][z_key]['data']) != list:
                output['domain']['axes']['z']['values']=[output['coords'][z_key]['data']] 
             else:
                output['domain']['axes']['z']['values']=output['coords'][z_key]['data']    
       for p in output['parameters']:
          output['ranges'][p]={}
          output['ranges'][p]['values']=np.array(output['parameters'][p]['data']).flatten().tolist()
          output['ranges'][p]['type']='NdArray'
          output['ranges'][p]['dataType']='float'
          output['parameters'][p]['description']={}
          output['parameters'][p]['description']['en']=output['parameters'][p]['attrs']['long_name']
          output['parameters'][p]['unit']={}
          output['parameters'][p]['unit']['symbol']={}
          output['parameters'][p]['unit']['symbol']['value']=output['parameters'][p]['attrs']['units']
          output['parameters'][p]['unit']['symbol']['type']=''
          output['parameters'][p]['unit']['label']={}
          output['parameters'][p]['unit']['label']['en']=output['parameters'][p]['attrs']['long_name']
          output['parameters'][p]['observedProperty']={}
          output['parameters'][p]['observedProperty']['label']={}
          output['parameters'][p]['observedProperty']['label']['en']=output['parameters'][p]['attrs']['long_name']
          del output['parameters'][p]['data']
          del output['parameters'][p]['dims']
          if isinstance(output['ranges'][p]['values'],list):
             iso_time=output['coords']['time']['data']
             output['domain']['axes']['t']={}
             iso_time_list=[x.isoformat() for x in iso_time] 
             output['domain']['axes']['t']['values']=iso_time_list
          else:
             count_for_time=len([output['ranges'][p]['values']])
       a=list()
       output['domain']["referencing"]=[]
       for p in output['parameters']:
          key_dir={}
          for k in list(output['domain']['axes'].keys()):
             if k != 'x' and k != 'y':
                if k not in a:
                   a.append(k)
             #Order matters, for polygon need t,y,x
             if k == 't':
                output['domain']["referencing"].append({"coordinates": ["t"],"system": {"type": "TemporalRS","calendar":"Gregorian"}})
             if k == 'z':
                output['domain']["referencing"].append({"coordinates": ["z"],"system": {"type": 'z info'}})
             if k == 'x':
                output['domain']["referencing"].append({"coordinates":["x","y"],"system":{"type": "GeographicCRS","id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}})
             key_dir[k]=len(output['domain']['axes'][k]['values'])
          a=sorted(a)
       #add referencing
       #need to take care of this by appending to list based on coordinates available:
       output['domain']["referencing"]=list()
       output['domain']["referencing"]=[{"coordinates": "feature_id", "system": {"id": [output['coords']['feature_id']['data']]}},{"coordinates":["y","x"],"system":{"type": "GeographicCRS","id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}},{"coordinates": ["t"],"system": {"type": "TemporalRS","calendar":"Gregorian"}}]
       del output['coords']       
       del output['attrs']
       del output['dims']
       return output


    def query(self,dataset, qtype, coords, time_range, z_value, params, instance, outputFormat):
       ds=self.load_data(self.zarr_store)
       output=ds[params]
       if time_range:
          start_date=np.datetime64(datetime.fromisoformat(str(time_range.get_start_date()).replace('Z','')))
          end_date=np.datetime64(datetime.fromisoformat(str(time_range.get_end_date()).replace('Z','')))
          output=output.sel({'time':slice(start_date,end_date)})
       if qtype=='point':
          output, output_boolean=get_point_data(self, output, outputFormat, coords, qtype)
          return output, output_boolean
       if qtype=='polygon':
          output, output_boolean=get_polygon_data(self,output, outputFormat, coords, qtype)
          return output, output_boolean
       if qtype_endpoint=='trajectory':
          output, output_boolean=get_trajectory_data(self, output, outputFormat, coords, qtype, params)
          return output, output_boolean
       if qtype_endpoint=='corridor':
          output, output_boolean=get_corridor_data(self, output, outputFormat, coords, qtype, params)
       return output, output_boolean



#Sampling Geometry Type Modules
#Eventually, even these modules can be separated out more to be included in the conversion modules


def get_point_data(self, output, outputFormat, coords, qtype):
   lon_lat=np.vstack((output.longitude.values,output.latitude.values)).T
   distance, index = spatial.KDTree(lon_lat).query(coords)   
   output=output.isel({'feature_id':index})
   output_dict=output.to_dict()
   if outputFormat=="CoverageJSON": #this if statement eventually will only occur once for all geom types
      output=self.pt_to_covjson(output_dict,coords,qtype)
      return json.dumps(output, indent=4, sort_keys=True, default=str).replace('NaN','null'), 'no_delete'
   if outputFormat=="NetCDF":
      output.to_netcdf(self.dir_root+'/output-'+self.uuid+'.nc')
      return flask.send_from_directory(self.dir_root,'output-'+self.uuid+'.nc',as_attachment=True), self.dir_root+'/output-'+self.uuid+'.nc'























