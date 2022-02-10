#!/opt/conda/envs/env/bin/python
import glob
import shutil
import xarray as xr
import re
import cfgrib
import argparse
from datetime import datetime, timedelta
import os
import uuid
from distributed import Client
import fsspec
import s3fs
import multiprocessing
import shutil

def expand_dims(d):
   d=d.expand_dims('step')
   return d


def create_collections(download_grib_location,zarr_output_location):
   ds_dict={}
   dims_dict={}
   ds_concat_dict={}
   ds_dict={}
   col_name_dict={}
   fs_list=list()
   convert_to_zarr(download_grib_location+'/combined.grb',zarr_output_location,col_name_dict,fs_list)
   return


def convert_to_zarr(f,zarr_output_location,col_name_dict,fs_list):
   client=Client(n_workers=10)
   ds_lists=cfgrib.open_datasets(f)
   col_name_list=list()
   f_name_list=list()
   for idxx,ds in enumerate(ds_lists):
      chunk_dict={}
      for dv in ds.data_vars:
          test_dv=dv
          break
      if 'step' in ds.dims:
         ds=ds.assign_coords({'step':ds.valid_time})
         ds=ds.drop('valid_time')
         ds=ds.rename({'step':'valid_time'})
      vert_coord=ds[dv].GRIB_typeOfLevel
      lv_coord_vals=ds[vert_coord].values.astype(str).tolist()
      if vert_coord not in ds.dims:
         ds=ds.expand_dims(vert_coord)
      coord_list=list()
      for c in ds.coords:
         if 'time' not in c and 'step' not in c and c != vert_coord:
            coord_list.append(c)
         if 'time' in c:
            time_coord=c
            len_vals=ds[c].values.tolist()
            if isinstance(len_vals,int):
               len_vals=[len_vals]
            time_len_val=str(len(len_vals))
            time_coord=c+time_len_val
            coord_list.append(time_coord)
         if c == vert_coord:
            vert_coord=c
            len_vals=ds[c].values.tolist()
            if isinstance(len_vals,int):
               len_vals=[len_vals]
            vert_len_val=str(len(len_vals))            
            vert_coord=c+vert_len_val
            coord_list.append(vert_coord)
      col_name_desc='gfs_100'+'_'+"_".join(coord_list)
      if col_name_desc not in col_name_list:
         col_name_list.append(col_name_desc)
      for dim in ds.dims:
         if 'latitude' in dim:
            chunk_dict[dim]=128
         if 'longitude' in dim:
            chunk_dict[dim]=128
         if vert_coord in dim:
            if int(vert_lev_val)>3:
               chunk_val=int(float(int(vert_lev_val)/3))
            chunk_dict[dim]=chunk_val
         if 'valid_time' in dim:
            if int(time_len_val)>3:
               chunk_val=int(float(int(time_len_val)/3))
            chunk_dict[dim]=chunk_val            
      for data_var in ds.data_vars:
         ds[data_var]=ds[data_var].chunk(chunks=chunk_dict)
      if col_name_desc in f_name_list:
         try:
            #ds.to_zarr(fsspec.get_mapper(zarr_output_location+col_name_desc,client_kwargs={'region_name':'us-east-1'}),mode='a',append_dim='valid_time')
            ds.to_zarr('./data/'+col_name_desc,mode='a',append_dim='valid_time')
            print(col_name_desc+' appended')
         except:
            try:
               col_name_desc=col_name_desc+'_alt'
               col_name_list.append(col_name_desc) 
               ds.to_zarr('./data/'+col_name_desc,mode='w')
            except:
               print('--------WARNING----------'+col_name_desc+' FAILED TO EXPORT TO ZARR')
      else:
         #ds.to_zarr(fsspec.get_mapper(zarr_output_location+col_name_desc,client_kwargs={'region_name':'us-east-1'}),mode='w')
         ds.to_zarr('./data/'+col_name_desc,mode='w')
         print(col_name_desc+' written')
      f_name_list.append(col_name_desc)
   return


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
   zarr_output_location='s3://enviroapi-bucket-1/zarr/'+model+'/'+model_run_time+'/'
   create_collections(download_grib_location,zarr_output_location)
