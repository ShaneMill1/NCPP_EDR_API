#!/opt/conda/envs/env/bin/python
import argparse
import glob
import multiprocessing
import shutil
import xarray as xr
import re
from distributed import Client
import numpy as np
from datetime import datetime
import os
import shutil
import json
import fsspec


def expand_dims(d):
   d=d.expand_dims('step')
   return d


def create_collections(download_grib_location,zarr_output_location,model):
   ds_dict={}
   dims_dict={}
   ds_concat_dict={}
   ds_dict={}
   ds=xr.open_dataset(download_grib_location+'/combined_data/combined.grb',engine='pynio')
   collection_dict={}
   client=Client(n_workers=10)
   for data_var in ds.data_vars:
      dv_ds=ds[data_var]
      dv_ds_params=list(dv_ds.dims)
      if 'level_type' in dv_ds.attrs.keys():
         dv_ds_params.append(dv_ds.level_type.replace(' ','_').lower())
      collection_name=model+'_'+'_'.join(dv_ds_params)
      collection_name=collection_name.replace('(','')
      collection_name=collection_name.replace(')','')
      if collection_name not in collection_dict.keys():
         collection_dict[collection_name]={}
         collection_dict[collection_name]['data_vars']=[]
      collection_dict[collection_name]['data_vars'].append(data_var)
   for key, value in collection_dict.items():
      col_dvs=collection_dict[key]['data_vars']
      col_ds=ds[col_dvs]
      if 'level' in ds[col_dvs[0]].attrs and 'lv_' not in key: 
         level_list=list()
         for dvl in col_ds:
            level_val='-'.join(map(str,col_ds[dvl].level.tolist()))
            if level_val not in level_list:
               level_list.append(level_val)
         for dvm in col_ds:
            level_val='-'.join(map(str,col_ds[dvm].level.tolist()))
            l_idx=str(level_list.index(level_val))
            dim_name='lv_'+l_idx
            try:
               col_ds[dvm]=col_ds[dvm].assign_coords({dim_name:level_val})
            except:
               pass
            col_ds[dvm]=col_ds[dvm].expand_dims(dim_name)
      if 'lv_' in key:
         for c_l in col_ds.dims:
            dim_vala=c_l+'_l0'
            dim_valb=c_l+'_l1'
            if dim_vala in ds.data_vars:
               dim_val_list=list()
               for idd,d in enumerate(ds[dim_vala].values):
                  dim_val=str(ds[dim_vala].values[idd])+'-'+str(ds[dim_valb].values[idd])
                  dim_val_list.append(dim_val)
               col_ds=col_ds.assign_coords({c_l:dim_val_list})

      for coord in col_ds.coords:
         if 'forecast_time' in coord:
            initial_time=col_ds[col_dvs[0]].initial_time
            initial_time=np.datetime64(find_initial_time(initial_time))
            time_vals=(initial_time+col_ds[coord].values).astype(str)
            col_ds=col_ds.assign_coords({coord:time_vals})
      convert_to_zarr(col_ds,collection_dict,key,value)
   client.close()
   return

def convert_to_zarr(col_ds,collection_dict,key,value):
   chunk_dict={}
   for dim in col_ds.dims:
      if 'lat_' in dim:
         chunk_dict[dim]=128
      if 'lon_' in dim:
         chunk_dict[dim]=128
      if 'lv_' in dim:
         chunk_dict[dim]=1
   col_ds=col_ds.chunk(chunks=chunk_dict)
   col_ds.to_zarr(fsspec.get_mapper(zarr_output_location+key,client_kwargs={'region_name':'us-west-2'}),mode='w',compute=True)
   print(key+' converted to zarr')
   return


def find_initial_time(initial_time):
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


if __name__ == "__main__":
   parser = argparse.ArgumentParser(description='Ingest Model')
   parser.add_argument('model',type=str,help='Enter the model, for example gfs_100')
   parser.add_argument('model_run', type = str, help = 'Enter model run (ie. 00 for 00z)')
   parser.add_argument('root_dir', type = str, help = 'root of the data directory, ie /data')
   args=parser.parse_args()
   model = args.model
   model_run=args.model_run
   root_dir=args.root_dir
   model_run_time=datetime.today().isoformat().split('T')[0]+'T'+model_run+':00:00'
   download_grib_location=root_dir+'/'+model+'/'+model_run_time
   zarr_output_location='s3://enviroapi-bucket-2/zarr/'+model+'/'+model_run_time+'/'
   create_collections(download_grib_location,zarr_output_location,model)

