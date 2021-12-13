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
           else:
              print('zarr datastore not ingested so query cannot be made')
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
 

    def pt_to_covjson(self,query_dict,coords,qtype):
       
       output=query_dict
       output['domain']={}
       output['type']="Coverage"
       output['domain']['type']="Domain"
       output['parameters'] = output.pop('data_vars')
       output['domain']['axes']={}
       output['domain']['axes']['x']={}
       output['domain']['axes']['y']={}
       if qtype=='point':
          output['domain']['domainType']="PointSeries"
          output['domain']['axes']['x']['values']=[output['coords']['lon_0']['data']]
          output['domain']['axes']['y']['values']=[output['coords']['lat_0']['data']]
       if qtype=='polygon':
          output['domain']['domainType']="Grid"
          output['domain']['axes']['y']['values']=output['coords']['lat_0']['data']
          output['domain']['axes']['x']['values']=output['coords']['lon_0']['data']
       del output['coords']['lat_0']['data']
       del output['coords']['lon_0']['data']
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
          if qtype=='polygon':
             output['ranges'][p]['values']=np.array(output['parameters'][p]['data']).flatten().tolist()
             #output['ranges'][p]['values']=output['parameters'][p]['data']
          else:
             output['ranges'][p]['values']=output['parameters'][p]['data']
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
       #Create iso dates from metadata and insert as t dimension for covjson
          initial_time=output['parameters'][p]['attrs']['initial_time']
          initial_time=self.initial_time(initial_time)
          if isinstance(output['ranges'][p]['values'],list):
             count_for_time=len(output['ranges'][p]['values'])
             f_timedelta=output['coords'][self.fkey]['data']
             iso_time=[]
             for t in f_timedelta:
                time=initial_time+t
                time=time.isoformat()
                iso_time.append(time)
             output['domain']['axes']['t']={}
             output['domain']['axes']['t']['values']=iso_time
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
          if 'x' in key_dir and 'y' in key_dir and 'z' in key_dir and 't' in key_dir:
             output['ranges'][p]['axisNames']=['t','z','y','x']
             output['ranges'][p]['shape']=[key_dir['t'],key_dir['z'],key_dir['y'],key_dir['x']]
          if 'x' in key_dir and 'y' in key_dir and 'z' not in key_dir and 't' in key_dir:
             output['ranges'][p]['axisNames']=['t','y','x']
             output['ranges'][p]['shape']=[key_dir['t'],key_dir['y'],key_dir['x']]
          if 'x' in key_dir and 'y' in key_dir and 'z' in key_dir and 't' not in key_dir:
             output['ranges'][p]['axisNames']=['z','y','x']
             output['ranges'][p]['shape']=[key_dir['z'],key_dir['y'],key_dir['x']]
          if 'x' in key_dir and 'y' in key_dir and 'z' not in key_dir and 't' not in key_dir:
             output['ranges'][p]['axisNames']=['y','x']
             output['ranges'][p]['shape']=[key_dir['y'],key_dir['x']]
       #add referencing
       #need to take care of this by appending to list based on coordinates available:
       output['domain']["referencing"]=list()
       output['domain']["referencing"]=[{"coordinates":["y","x"],"system":{"type": "GeographicCRS","id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}},{"coordinates": ["t"],"system": {"type": "TemporalRS","calendar":"Gregorian"}}]

       del output['coords']       
       del output['attrs']
       del output['dims']
       return output


    def query(self,dataset, qtype, coords, time_range, z_value, params, instance, outputFormat):
       #using the collection meta, make query based on the dimensions, so the automated process would be to formulate kwargs from the dimension list
       #important coords is an array of [lon,lat]
       #correct lon to fit 0->360
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
        
       for key in dir(ds[params[0]]):
          if "forecast_time" in key:
             self.fkey = key
          if "lv" in key:
             self.lvkey=key
          if "lon_" in key:
             self.lonkey=key
          if "lat_" in key:
             self.latkey=key
       if self.fkey:
          forecast_time_key=self.fkey
          try:
             arguments={forecast_time_key: slice(start_delta,end_delta)}
             output=ds[params].sel(**arguments)
          except:
             output=ds[params]
       if z_value:
          if 'e' in z_value:
             z_array=z_value.split('e')
             z_value=float(z_array[0])*10**int(z_array[1])
          if '-' in z_value:
             url=flask.request.url
             url_component=url.split('?')
             url_slashes=url_component[0].split('/')
             url_delete_comp=url_slashes[len(url_slashes)-1]
             meta=url_component[0].replace('/'+url_delete_comp,'')+'?f=application/json'
             with urllib.request.urlopen(meta) as murl:
                data = json.loads(murl.read().decode())
             levels_meta=data['parameters'][params[0]]['extent']['vertical']['range']
             for idx,l in enumerate(levels_meta):
                if l == z_value:
                   z_value=idx
                else:
                   pass
          if '/' in z_value:
             z=z_value.split('/')
             try:
                z[0]=int(z[0])
                z[1]=int(z[1])
             except:
                z[0]=float(z[0])
                z[1]=float(z[1])
             z_arg={self.lvkey: slice(z[0],z[1])}
          else:
             try:
                z_value=int(z_value)
             except:
                z_value=float(z_value)
             z_arg={self.lvkey: z_value}
          output=output.sel(**z_arg)
       if qtype=='point':
          query_args={self.latkey: int(coords[1]), self.lonkey: int(coords[0])}
          lonattr=str(self.lonkey)
          output=output.assign_coords({self.lonkey: getattr(output,lonattr) - 360})
          output=output.rio.set_spatial_dims(self.lonkey,self.latkey,inplace=True)
          output=output.rio.write_crs(4326)
          output=output.sel(query_args, method='nearest')
          output=output.to_dict()
          if outputFormat=="CoverageJSON":
             output=self.pt_to_covjson(output,coords,qtype)
             return json.dumps(output, indent=4, sort_keys=True, default=str).replace('NaN','null')
       if qtype=='polygon':
          geometries=[];coord_list=list()
          geometries.append({'type':'Polygon', 'coordinates':[coords]})
          lonattr=str(self.lonkey)
          output_adl=output
          output=output.assign_coords({self.lonkey: getattr(output,lonattr) - 360})
          output_bdl=output
          output=xr.concat([output_bdl,output_adl],dim='lon_0')
          output=output.rio.set_spatial_dims(self.lonkey,self.latkey,inplace=True)
          output=output.rio.write_crs(4326)
          output=output.rio.clip(geometries,output.rio.crs)
          output=output.to_dict()
          if outputFormat=="CoverageJSON":
             output=self.pt_to_covjson(output,coords,qtype)
             return json.dumps(output, indent=4, sort_keys=True, default=str).replace('NaN','null')
          if outputFormat=="GRIB":
             grib_template=self.DATASET_FOLDER+'/'+self.model+'/'+self.cycle+'/'+self.cycle+'_'+self.model+'_template.grib'
             convert_to_grib.create_grib(output,grib_template)
             output={'result': 'grib file should display for download'}
             return flask.send_from_directory('/data','output.grb',as_attachment=True)
       if qtype=='cube':
          geometries=[];coord_list=list()
          geometries.append({'type':'Polygon', 'coordinates':[coords]})
          lonattr=str(self.lonkey)
          output_adl=output
          output=output.assign_coords({self.lonkey: getattr(output,lonattr) - 360})
          output_bdl=output
          output=xr.concat([output_bdl,output_adl],dim='lon_0')
          output=output.rio.set_spatial_dims(self.lonkey,self.latkey,inplace=True)
          output=output.rio.write_crs(4326)
          output=output.rio.clip(geometries,output.rio.crs)
          output=output.to_dict()
          if outputFormat=="CoverageJSON":
             output=self.pt_to_covjson(output,coords,qtype)
             return json.dumps(output, indent=4, sort_keys=True, default=str).replace('NaN','null')
          if outputFormat=="GRIB":
             grib_template=self.DATASET_FOLDER+'/'+self.model+'/'+self.cycle+'/'+self.cycle+'_'+self.model+'_template.grib'
             convert_to_grib.create_grib(output,grib_template)
             output={'result': 'grib file should display for download'}
             return flask.send_from_directory('/data','output.grb',as_attachment=True)


