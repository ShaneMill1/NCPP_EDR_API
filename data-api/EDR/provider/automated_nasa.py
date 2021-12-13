from datetime import datetime
import xarray as xr
import cftime
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
           else:
              print('zarr datastore not ingested so query cannot be made')
       zarr_obj=xr.open_zarr(c_zarr,decode_times=False,mask_and_scale=False)
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
 

    def pt_to_covjson(self,query_dict,coords):
       
       output=query_dict
       output['domain']={}
       output['type']="Coverage"
       output['domain']['type']="Domain"
       output['domain']['domainType']="PointSeries"
       output['parameters'] = output.pop('data_vars')
       output['domain']['axes']={}
       output['domain']['axes']['x']={}
       output['domain']['axes']['x']['values']=[coords[0]]
       del output['coords']['lat']['data']
       output['domain']['axes']['y']={}
       output['domain']['axes']['y']['values']=[coords[1]]
       del output['coords']['lon']['data']
       output['ranges']={}
       #find dim with "lv" and create the "z" dimension
       for key in output['coords']:
          if 'lv' in key:
             z_key=key
             output['domain']['axes']['z']={}
             output['domain']['axes']['z']['values']=[output['coords'][z_key]['data']]       
       
       for p in output['parameters']:
          output['parameters'][p]['attrs']['DIMENSION_LIST']=[]
          output['ranges'][p]={}
          output['ranges'][p]['values']=output['parameters'][p]['data']
          output['ranges'][p]['type']='NdArray'
          output['ranges'][p]['dataType']='float'
          output['parameters'][p]['description']={}
          #output['parameters'][p]['description']['en']=output['parameters'][p]
          output['parameters'][p]['unit']={}
          output['parameters'][p]['unit']['symbol']={}
          output['parameters'][p]['unit']['symbol']['value']=output['parameters'][p]['attrs']['units']
          output['parameters'][p]['unit']['symbol']['type']=''
          output['parameters'][p]['unit']['label']={}
          output['parameters'][p]['unit']['label']['en']=p
          output['parameters'][p]['observedProperty']={}
          output['parameters'][p]['observedProperty']['label']={}
          output['parameters'][p]['observedProperty']['label']['en']=p
          del output['parameters'][p]['data']
          del output['parameters'][p]['dims']
       #Create iso dates from metadata and insert as t dimension for covjson
       output['domain']['axes']['t']={}
       time_list=list()
       for tm in output['coords']['time']['data']:
          try:
             time=str(tm.strftime()).replace(' ','T')
          except:
             time=tm
          time_list.append(time)
       output['domain']['axes']['t']['values']=time_list
       count_for_time=len(output['domain']['axes']['t']['values'])
       a=list()
       output['domain']["referencing"]=[]
       for p in output['parameters']:
          for k in list(output['domain']['axes'].keys()):
             if k != 'x' and k != 'y':
                if k not in a:
                   a.append(k)
             if k == 'x':
                output['domain']["referencing"].append({"coordinates":["x","y"],"system":{"type": "GeographicCRS","id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}})
             if k == 't':
                output['domain']["referencing"].append({"coordinates": ["t"],"system": {"type": "TemporalRS","calendar":"Gregorian"}})
             if k == 'z':
                output['domain']["referencing"].append({"coordinates": ["z"],"system": {"type": 'z info'}})
          a=sorted(a)
          if 'z' in a:
             output['ranges'][p]['shape']=[count_for_time,1]
          else:
             output['ranges'][p]['shape']=[count_for_time]
          output['ranges'][p]['axisNames']=a
             

       #add referencing
       #need to take care of this by appending to list based on coordinates available:
       output['domain']["referencing"]=list()
       output['domain']["referencing"]=[{"coordinates":["x","y"],"system":{"type": "GeographicCRS","id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}},{"coordinates": ["t"],"system": {"type": "TemporalRS","calendar":"Gregorian"}},{"coordinates": ["t"],"system": {"type": "TemporalRS","calendar":"Gregorian"}}]

       del output['coords']       
       del output['attrs']
       del output['dims']
       return output


    def query(self,dataset,qtype, coords, time_range, z_value, params, instance, outputFormat):
       #using the collection meta, make query based on the dimensions, so the automated process would be to formulate kwargs from the dimension list
       #important coords is an array of [lon,lat]
       #correct lon to fit 0->360
       coords[0]=coords[0]+360
       if coords[0]>360:
          coords[0]=coords[0]-360
       cid="_".join(dataset.split("_", 2)[1:])
       model=dataset.split("_")[1]+'_'+dataset.split("_")[2]
       ds=self.load_data(cid,instance,model)
       self.load_collection_meta(cid)
       dim_key=self.dimensions
       start=str(time_range.get_start_date()).replace('T',' ')
       start=start.replace('Z','')
       start=start.replace('-',':')
       start=start.replace(' ',':') 
       start=start.split(':')
       start=cftime.DatetimeJulian(int(start[0]),int(start[1]),int(start[2]),int(start[3]),int(start[4]),int(start[5]))
       end=str(time_range.get_end_date()).replace('T',' ')
       end=end.replace('Z','')
       end=end.replace('-',':')
       end=end.replace(' ',':')
       end=end.split(':')
       end=cftime.DatetimeJulian(int(end[0]),int(end[1]),int(end[2]),int(end[3]),int(end[4]),int(end[5]))
       for key in dir(ds[params[0]]):
          if "time" in key:
             self.fkey = key
          if "lv" in key:
             self.lvkey=key
          if "lon" in key and "long_name" not in key:
             self.lonkey=key
          if "lat" in key:
             self.latkey=key
       if self.fkey:
          if 'imerg' in dataset:   
             start=start._to_real_datetime().timestamp()
             end=end._to_real_datetime().timestamp()
             forecast_time_key=self.fkey
             arguments={forecast_time_key: slice(start,end)}
             output=ds[params].sel(**arguments)
          if 'merra' in dataset:
             forecast_time_key=self.fkey
             begin_date=ds['time'].begin_date
             begin_date=begin_date.replace('[','')
             begin_date=begin_date.replace(']','')
             my_date=str(datetime.strptime(begin_date,"%Y%m%d"))
             my_date=my_date.replace('-',':')
             my_date=my_date.replace(' ',':')
             my_date=my_date.split(':')
             my_date=cftime.DatetimeJulian(int(my_date[0]),int(my_date[1]),int(my_date[2]),int(my_date[3]),int(my_date[4]),int(my_date[5]))
             start_f=start-my_date
             start_f=int(start_f.seconds/60)
             end_f=end-my_date
             end_f=int(end_f.seconds/60)
             arguments={forecast_time_key: slice(start_f,end_f)}
             output=ds[params].sel(**arguments)
       if z_value:
          if 'e' in z_value:
             z_array=z_value.split('e')
             z_value=float(z_array[0])*10**int(z_array[1])
          try:
             z_value=int(z_value)
          except:
             z_value=float(z_value)
          z_arg={self.lvkey: z_value}
          output=output.sel(**z_arg)
       query_args={self.latkey: int(coords[1]), self.lonkey: int(coords[0])}
       output=output.sel(query_args, method='nearest')
       output=output.to_dict()
       if qtype=='point':
          output=self.pt_to_covjson(output,coords)
       return json.dumps(output, indent=4, sort_keys=True, default=str).replace('NaN','null'), 'no_delete'


