from datetime import datetime
import xarray as xr
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

class AutomatedCollectionProvider(BaseProvider):

    def __init__(self,dataset,config):
        """initializer"""
        dsn=dataset.split('_')
        ds_name=dsn[0]+'_'+dsn[1]
        self.DATASET_FOLDER = config['datasets'][ds_name]['provider']['data_source']

    
    def load_data(self,cid,instance,model):
       #Creates a dictionary with collection ID as key and zarr collection object as value so that the specific 
       #zarr object (dataset) corresponding to a collection ID can be returned.
       self.model=model
       self.cycle=instance
       self.zarr_store=self.DATASET_FOLDER+'/'+self.model+'/'+self.cycle+'/zarr/*'
       self.col_json=self.DATASET_FOLDER+'/'+self.model+'/'+self.cycle+'/'+self.cycle+'_'+self.model+'_collection.json'
       self.data_files=[]
       for c in sorted(glob.iglob(self.zarr_store)):
           if os.path.basename(c) == cid:
              c_zarr=c
       zarr_obj=xr.open_zarr(c_zarr)
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
       try:
          self.level_type=collection['level_type']
       except:
          self.level_type='level_type_na'
       self.long_name=collection['long_name'] 
       return
    
    def initial_time(self,initial_time):
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


    def query(self,dataset, qtype, coords, time_range, z_value, params, instance, outputFormat):
       cid="_".join(dataset.split("_", 2)[1:])
       model=dataset.split("_")[1]+'_'+dataset.split("_")[2]
       ds=self.load_data(cid,instance,model)
       self.load_collection_meta(cid)
       dim_key=self.dimensions
       try:
          initial_time=ds[params[0]].initial_time
          initial_time=self.initial_time(initial_time)
          start=datetime.strptime(str(time_range.get_start_date()), "%Y-%m-%dT%H:%M:%SZ")
          end=datetime.strptime(str(time_range.get_end_date()), "%Y-%m-%dT%H:%M:%SZ")
          start_delta=start-initial_time      
          end_delta=end-initial_time
       except:
          pass
        
       if qtype=='point':
          output=ds[params]
          query_args_0={'xgrid_0': int(coords[1]), 'ygrid_0': int(coords[0])}
          query_args_1={'xgrid_1': int(coords[1]), 'ygrid_1': int(coords[0])}
          query_args_2={'xgrid_2': int(coords[1]), 'ygrid_2': int(coords[0])}
          try: 
             output=output.sel(query_args_0)
          except:
             pass
          try:
             output=output.sel(query_args_1)
          except:
             pass
          try:
             output=output.sel(query_args_2)
          except:
             pass
          
          i=0
          while i<29:
             forecast_time_key='forecast_time'+str(i)
             try:
                arguments={forecast_time_key: slice(start_delta,end_delta)}     
                output=output.sel(**arguments)    
             except:
                pass
             i=i+1
          output=output.to_dict()
          if outputFormat=="json":
             return json.dumps(output, indent=4, sort_keys=True, default=str).replace('NaN','null'), 'no_delete'


