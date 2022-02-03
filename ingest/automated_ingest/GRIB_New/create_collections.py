#!/opt/conda/envs/env/bin/python
import glob
import shutil
import xarray as xr
import re
import cfgrib
import argparse
from datetime import datetime
import os
import uuid
from distributed import Client
import fsspec

def expand_dims(d):
   d=d.expand_dims('valid_time')
   return d


def create_collections(download_grib_location,zarr_output_location):
   client=Client(n_workers=5)
   ds_dict={}
   dims_dict={}
   ds_concat_dict={}
   ds_dict={}
   col_name_dict={}
   fs_list=list()
   files=glob.glob(download_grib_location+'/*[!.idx]')
   files.sort(key=os.path.getmtime)
   for idx,f in enumerate(files):
      convert_to_zarr(f,zarr_output_location,col_name_dict,fs_list)
   return


def convert_to_zarr(f,zarr_output_location,col_name_dict,fs_list):
   chunk_dict={}
   ds_lists=cfgrib.open_datasets(f)
   for idxx,ds in enumerate(ds_lists):
      ds=ds.expand_dims('valid_time')
      ds=ds.drop('time');ds=ds.drop('step')
      for coord in ds.coords:
         if coord != 'valid_time' and coord != 'latitude' and coord != 'longitude':
            lv_coord_vals=ds[coord].values.astype(str).tolist()
            if isinstance(lv_coord_vals,list):
               lv_coord_val_string='_'.join(lv_coord_vals)
            else:
                lv_coord_val_string=str(float(lv_coord_vals))
            try:
               ds=ds.expand_dims(coord)
            except:
               pass
      col_name_desc="_".join(list(ds.coords))+'_'+lv_coord_val_string
      if col_name_desc not in col_name_dict.keys():
         col_name_id=str(uuid.uuid4())
         col_name_dict[col_name_desc]=col_name_id
      for dim in ds.dims:
         if 'latitude' in dim:
            chunk_dict[dim]=64
         if 'longitude' in dim:
            chunk_dict[dim]=64
      for data_var in ds.data_vars:
         ds[data_var]=ds[data_var].chunk(chunks=chunk_dict)
      if col_name_desc in fs_list:
         try:
             ds.to_zarr(fsspec.get_mapper(zarr_output_location+col_name_dict[col_name_desc],client_kwargs={'region_name':'us-east-1'})),mode='a',append_dim='valid_time')
            print(col_name_dict[col_name_desc]+' appended')
         except:
            print('--------WARNING----------'+col_name_dict[col_name_desc]+' FAILED TO EXPORT TO ZARR')
      else:
          ds.to_zarr(fsspec.get_mapper(zarr_output_location+col_name_dict[col_name_desc],client_kwargs={'region_name':'us-east-1'})),mode='w')
         print(col_name_dict[col_name_desc]+' written')
      if col_name_desc not in fs_list:
         fs_list.append(col_name_desc)
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
