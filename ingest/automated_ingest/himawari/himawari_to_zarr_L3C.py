#!/opt/conda/envs/env/bin/python
import argparse
import numpy as np
import xarray as xr
import s3fs
import time
import fsspec
from datetime import datetime
from distributed import Client
import dask
from dask.diagnostics import ProgressBar
import os
import zarr
from create_meta import meta
import shutil
from pyproj import CRS



def open_from_s3(s3_endpoint,output_dir):
   fs = s3fs.S3FileSystem(anon=True)
   remote_latest_year=fs.glob(s3_endpoint+'/*')[-1]
   remote_latest_month=fs.glob(remote_latest_year+'/*')[-1]
   remote_latest_day=fs.glob(remote_latest_month+'/*')[-1]
   remote_latest_hour=fs.glob(remote_latest_day+'/*')[-1]
   remote_latest_hour_contents=fs.glob(remote_latest_hour+'/*')
   fileset=list()
   for f in remote_latest_hour_contents:
      if 'L3C' in f:
         remote_f=fs.open(f)
         fileset.append(remote_f)
      else:
         pass
   print('open netcdf files into xarray dataset')
   client = Client()
   #ds=xr.open_mfdataset(fileset,concat_dim='time',preprocess=set_time)
   #ds=move_data_vars(ds)
   ds=xr.open_dataset(fileset[0])
   ds=process_data(ds)
   out=ds.to_zarr(output_dir,compute=False)
   with ProgressBar():
      results=out.compute()
   return

def process_data(ds):
   ds=ds['sea_surface_temperature'].to_dataset()
   return ds


if __name__ == '__main__':
   today=datetime.today()
   parser = argparse.ArgumentParser(description='Ingest GOES and create collections')
   parser.add_argument('collection_name', type = str, help = 'Enter himawari collection')
   args=parser.parse_args()
   collection_name='himawari-'+args.collection_name
   output_dir='/data/collections/'+collection_name+'/zarr'
   if os.path.exists(output_dir) and os.path.isdir(output_dir):
      shutil.rmtree(output_dir)
   meta_location='/data/collections/'+collection_name+'/'
   s3_name=collection_name.replace('himawari-','')
   s3_endpoint='s3://noaa-himawari8/'+s3_name
   open_from_s3(s3_endpoint,output_dir)
   meta_output='/data/collections/'+collection_name
   meta(meta_output,collection_name)
