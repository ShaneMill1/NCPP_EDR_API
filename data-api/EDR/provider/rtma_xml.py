import datetime
import xarray as xr
from xarray import DataArray
import copy
import glob
import json
from shapely.geometry import Point, Polygon
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

class RTMAXmlProvider(BaseProvider):

    def __init__(self,dataset,config):
        """initializer"""
        dsn=dataset.split('_')
        ds_name=dsn[0]+'_'+dsn[1]
        self.DATASET_FOLDER = config['datasets'][ds_name]['provider']['data_source']
        self.dir_root=self.DATASET_FOLDER.replace('/collections','')

    
    def load_data(self,cid,instance,model):
       #Creates a dictionary with collection ID as key and zarr collection object as value so that the specific 
       #zarr object (dataset) corresponding to a collection ID can be returned.
       self.model=model
       self.cycle=instance
       self.zarr_store=self.DATASET_FOLDER+'/'+self.model+'/'+self.cycle+'/zarr'
       zarr_obj=xr.open_zarr(self.zarr_store)
       return zarr_obj
    
   
    def load_collection_meta(self,cid):
       #loads the collections json containing the metadata for a model run time which maps collection ID to the parameters, dimensions
       # level type, long_name, etc. 
       with open(self.col_json, 'r') as col_json:
          col=json.load(col_json)
       for idx,c in enumerate(col):
          if col[idx]['collection_name']==cid:
             collection=c
       self.parameters=collection['parameters']
       self.dimensions=collection['dimensions']
       self.level_type=collection['level_type']
       self.long_name=collection['long_name'] 
       return
    
    def query(self,dataset, qtype, coords, time_range, z_value, params, instance, outputFormat):
       dir_root=self.dir_root
       lv_list=list();param_list=list()
       model=dataset;cid=dataset
       output=self.load_data(cid,instance,model)
       self.gridlat='gridlat_0'
       self.gridlon='gridlon_0'
       self.xgrid='xgrid_0'
       self.ygrid='ygrid_0'
       if qtype=='point':
          output=self.get_point(output,coords,params,time_range)
       if qtype=='multipoint':
          output=self.get_multipoint(output,coords,params,time_range)
       output=output.to_dict()
       output=self.clean_output(output)
       return json.dumps(output, indent=4, sort_keys=True, default=str).replace('NaN','null'), 'no_delete'


    def get_point(self,output,coords,params,time_range):
          if len(params)>0:
             output=output[params]
          else:
             output=output
          lat=coords[1]
          lon=coords[0]
          coord_dict={}
          gridlat=self.gridlat
          gridlon=self.gridlon
          abslat = np.abs(output[gridlat] - lat)
          abslon = np.abs(output[gridlon] - lon)
          c = np.maximum(abslat, abslon)
          del gridlat;del gridlon
          # Get the x/y index location
          nearestPoints=np.where(c==np.min(c))
          yloc=nearestPoints[0].item(0)
          xloc=nearestPoints[1].item(0)
          coord_dict['xgrid_0']=xloc
          coord_dict['ygrid_0']=yloc
          if time_range:
             start=str(time_range.get_start_date()).replace('Z','')
             end=str(time_range.get_end_date()).replace('Z','')
             for dv in output.data_vars:
                ftime_param='forecast_time0_'+dv
                output=output.sortby(ftime_param)
                output=output.sel({ftime_param:slice(str(start),str(end))})
          output=output.sel(coord_dict,method='nearest')
          return output


    def get_multipoint(self,output,coords,params,time_range):
          start=str(time_range.get_start_date()).replace('Z','')
          end=str(time_range.get_end_date()).replace('Z','')
          coord_sel={};xloc_list=list();yloc_list=list()
          if len(params)>0:
             output=output[params]
          else:
             output=output
          for m in coords:
             lat=float(float(m[1]))
             lon=float(float(m[0]))
             coord_dict={}
             dv_list=list()
             gridlat=self.gridlat
             gridlon=self.gridlon
             abslat = np.abs(output[gridlat] - lat)
             abslon = np.abs(output[gridlon] - lon)
             c = np.maximum(abslat, abslon)
             del gridlat;del gridlon
             # Get the x/y index location
             nearestPoints=np.where(c==np.min(c))
             yloc=nearestPoints[0].item(0)
             xloc=nearestPoints[1].item(0)
             coord_dict['xgrid_0']=xloc
             coord_dict['ygrid_0']=yloc
             xloc_list.append(xloc)
             yloc_list.append(yloc)
             coord_sel['xgrid_0']=xr.DataArray(xloc_list)
             coord_sel['ygrid_0']=xr.DataArray(yloc_list)  
          if time_range:
             for dv in output.data_vars:
                ftime_param='forecast_time0_'+dv
                output=output.sortby(ftime_param)
                output=output.sel({ftime_param:slice(str(start),str(end))})
          output=output.sel(coord_sel,method='nearest')
          return output

    def clean_output(self,output):
       clean_output=copy.deepcopy(output)
       #clean_output['data_vars']['points']={}
       clean_output['data_vars']['elements']={}
       del clean_output['attrs']
       del clean_output['dims']
       for key in output['coords']:
          if 'forecast_time' in key:
             ftimes=clean_output['coords'][key]['data']
             clean_output['coords'][key]=ftimes
          else:
             del clean_output['coords'][key]
       for key in output['data_vars']:
          clean_output['data_vars']['elements'][key]=copy.deepcopy(output['data_vars'][key]['data'])
          #clean_output['data_vars']['lat']=copy.deepcopy(output['coords']['gridlat_0_'+key]['data'])   
          #clean_output['data_vars']['lon']=copy.deepcopy(output['coords']['gridlon_0_'+key]['data'])
          del clean_output['data_vars'][key]
       return clean_output



