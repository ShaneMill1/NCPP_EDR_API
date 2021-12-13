import datetime
import xarray as xr
from xarray import DataArray
import copy
import glob
import json
from shapely.geometry import Point, Polygon
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
import subprocess
from distributed import Client

class NDFDXmlProvider(BaseProvider):

    def __init__(self,dataset,config):
        """initializer"""
        dsn=dataset.split('_')
        ds_name=dsn[0]+'_'+dsn[1]
        self.DATASET_FOLDER = config['datasets'][ds_name]['provider']['data_source']
        self.dir_root=self.DATASET_FOLDER.replace('/collections','')
   
 
    def query(self,dataset, qtype, coords, time_range, z_value, params, instance, outputFormat):
       dir_root=self.dir_root
       lv_list=list();param_list=list()
       model=dataset;cid=dataset
       #available sector datasets
       if qtype=='multipoint':
          sector_ds_dict={}
          for idc,c in enumerate(coords):
             lat=c[1];lon=c[0]
             sector_list=subprocess.check_output(['./degrib','-Sector','-pnt',str(lat)+','+str(lon)]).decode('utf-8').replace("\n","/").split('/')
             sector_update_list=list()
             for sec in sector_list:
                if 'conus' in sec:
                   sec='conus'
                if sec == '': 
                    pass
                if sec not in sector_update_list and len(sec)>0:
                   sector_update_list.append(sec)
             sector_ds_dict[idc]=sector_update_list
       if qtype == 'point':
          lat=coords[1];lon=coords[0]
          sector_list=subprocess.check_output(['./degrib','-Sector','-pnt',str(lat)+','+str(lon)]).decode('utf-8').replace("\n","/").split('/')
          sector_update_list=list()
          for sec in sector_list:
             if 'conus' in sec:
                sec='conus'
             if sec not in sector_update_list and len(sec)>0:
                sector_update_list.append(sec)
          sector_ds_dict={0: sector_update_list}
       if qtype=='point':
          output=self.get_point(sector_ds_dict,coords,params,time_range)
          output=output.to_dict()
          output=self.clean_output(output)
       if qtype=='multipoint':
          output_list=self.get_multipoint(sector_ds_dict,coords,params,time_range)
          output_ds=xr.combine_nested(output_list,concat_dim='point_index',coords='minimal',compat='override')
          output_dict=output_ds.to_dict()
          output_dict=self.clean_output(output_dict)
          for data_var in output_ds.data_vars:
              try:
                 output_dict['data_vars']['elements'][data_var]=[list(t) for t in zip(*output_dict['data_vars']['elements'][data_var])]
              except:
                 new_output_list=list()
                 for o_list in output_list:
                    if data_var in o_list.data_vars:
                       new_output_list.append(output_dict['data_vars']['elements'][data_var])
                    else:
                       null_list=[None]*len(output_dict['data_vars']['elements'][data_var])
                       new_output_list.append(null_list)
                 output_dict['data_vars']['elements'][data_var]=[list(t) for t in zip(*new_output_list)]
          output=output_dict
       if outputFormat=='json':
          return json.dumps(output, indent=4, sort_keys=True, default=str).replace('NaN','null'), 'no_delete'
       if outputFormat=='text':
          output=output.__str__().replace(' ','')
          output=output.replace('nan','null')
          output=output.replace("'",'"')
          return output, 'no_delete'



    def get_point(self,sector_ds_dict,coords,params,time_range):
          output_list=list();sector_list=sector_ds_dict[0]
          for sector in sector_list:
             if os.path.exists(self.dir_root+'/collections/ndfd_xml/'+sector):
                output=xr.open_zarr(self.dir_root+'/collections/ndfd_xml/'+sector+'/latest/zarr')
                if len(params)>0:
                   try:
                      output=output[params]
                   except:
                      break
                if sector == 'guam' or sector == 'hawaii' or sector == 'puertori':
                   test_coords=[]
                   if sector == 'puertori' or sector == 'hawaii':
                      new_coord=coords[0]+360
                   else:
                      new_coord=coords[0]
                   test_coords=[new_coord,coords[1]]
                   point=Point(test_coords)
                elif sector == 'conus' or sector == 'alaska' or sector == 'npacocn' or sector == 'oceanic':
                   if sector=='npacocn':
                      new_coord=coords[0]+360
                      test_coords=[new_coord,coords[1]]
                      point=Point(test_coords)
                   if sector == 'oceanic':
                      new_coord=coords[0]+360
                      test_coords=[new_coord,coords[1]]
                      point=Point(test_coords)
                   if sector=='alaska':
                      point=Point(coords)
                   else:
                      point=Point(coords)
                coord_dict={}
                lat=point.y
                lon=point.x
                if 'gridlat_0' in output.coords:
                   abslat = np.abs(output['gridlat_0'] - lat)
                   abslon = np.abs(output['gridlon_0'] - lon)
                   c = np.maximum(abslat, abslon)
                   # Get the x/y index location
                   nearestPoints=np.where(c==np.min(c))
                   yloc=nearestPoints[0].item(0)
                   xloc=nearestPoints[1].item(0)
                   coord_dict['ygrid_0']=yloc
                   coord_dict['xgrid_0']=xloc
                else:
                   coord_dict['lat_0']=lat
                   coord_dict['lon_0']=lon
                for coords_name in output.coords:
                   if 'forecast_time' in coords_name:
                      output=output.sortby(coords_name)
                if time_range:
                   for dv in output.data_vars:
                      ftime_param='forecast_time0_'+dv
                      output=output.sel({ftime_param:slice(time_range[0],time_range[1])})
                output=output.sel(coord_dict,method='nearest')
                output_list.append(output)
             final_output=xr.merge(output_list,compat='override')
          else:
             pass
          return final_output


    def get_multipoint(self,sector_ds_dict,coords,params,time_range):
       final_output_list=list()
       for idc,coordf in enumerate(coords):
          coord=list()
          for c in coordf:
             coord.append(float(c))
          sector_list=sector_ds_dict[idc];output_list=list()
          for sector in sector_list:
             if os.path.exists(self.dir_root+'/collections/ndfd_xml/'+sector):
                output=xr.open_zarr(self.dir_root+'/collections/ndfd_xml/'+sector+'/latest/zarr')
                if len(params)>0:
                   try:
                      output=output[params]
                   except:
                      break
                if sector == 'guam' or sector == 'hawaii' or sector == 'puertori':
                   test_coords=[]
                   if sector == 'puertori' or sector == 'hawaii':
                      new_coord=coord[0]+360
                   else:
                      new_coord=coord[0]
                   test_coords=[new_coord,coord[1]]
                   point=Point(test_coords)
                elif sector == 'conus' or sector == 'alaska' or sector == 'npacocn' or sector == 'oceanic':
                   if sector=='npacocn':
                      new_coord=coord[0]+360
                      test_coords=[new_coord,coord[1]]
                      point=Point(test_coords)
                   if sector == 'oceanic':
                      new_coord=coord[0]+360
                      test_coords=[new_coord,coord[1]]
                      point=Point(test_coords)
                   if sector=='alaska':
                      point=Point(coord)
                   else:
                      point=Point(coord)
                coord_dict={}
                lat=point.y
                lon=point.x
                if 'gridlat_0' in output.coords:
                   abslat = np.abs(output['gridlat_0'] - lat)
                   abslon = np.abs(output['gridlon_0'] - lon)
                   c = np.maximum(abslat, abslon)
                   # Get the x/y index location
                   nearestPoints=np.where(c==np.min(c))
                   yloc=nearestPoints[0].item(0)
                   xloc=nearestPoints[1].item(0)
                   coord_dict['ygrid_0']=yloc
                   coord_dict['xgrid_0']=xloc
                else:
                   coord_dict['lat_0']=lat
                   coord_dict['lon_0']=lon
                for coords in output.coords:
                   if 'forecast_time' in coords:
                      output=output.sortby(coords)
                if time_range:
                   for dv in output.data_vars:
                      ftime_param='forecast_time0_'+dv
                      output=output.sel({ftime_param:slice(time_range[0],time_range[1])})
                output=output.sel(coord_dict,method='nearest')
                output_list.append(output)
             final_output=xr.merge(output_list,compat='override')
             final_output_list.append(final_output)
          else:
             pass
       return final_output_list


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


