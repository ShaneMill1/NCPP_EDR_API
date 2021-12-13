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
from netCDF4 import Dataset
import os
from create_meta import meta

def open_from_s3(s3_endpoint,output_dir):
   fs = s3fs.S3FileSystem(anon=True)
   remote_files=fs.glob(s3_endpoint+'/*')
   fileset = [fs.open(file) for file in remote_files][0:3]
   client = Client()
   print('open netcdf files into xarray dataset')
   ds=xr.open_mfdataset(fileset,concat_dim='time',preprocess=set_time)
   print('extracting initial metadata')
   with fs.open(fileset[0], 'rb') as ff:
      ds_extract= xr.open_dataset(ff)
   print('testing setting dims')
   for dv in ds.data_vars:
      if 'x' in ds[dv].dims and 'y' in ds[dv].dims:
         pass
      else:
         ds=ds.drop(dv)
   print('setting lat and lon as coord')
   ds=ds.rename({'y':'Rows'})
   ds=ds.rename({'x':'Columns'})
   ds=ds.assign_coords({'Latitude':ds_extract['Latitude']})
   ds=ds.assign_coords({'Longitude':ds_extract['Longitude']})
   ds=ds.rename({'Rows':'y'})
   ds=ds.rename({'Columns':'x'})
   lat_min=np.nanmin(ds.Latitude.values)
   lat_max=np.nanmax(ds.Latitude.values)
   lon_min=np.nanmin(ds.Longitude.values)
   lon_max=np.nanmax(ds.Longitude.values)
   y_size=ds.y.values.size
   x_size=ds.x.values.size
   lat_dim=np.linspace(lat_min,lat_max,y_size)
   lon_dim=np.linspace(lon_min,lon_max,x_size)
   ds=ds.assign_coords({'lon_val':lon_dim})
   ds=ds.assign_coords({'lat_val':lat_dim})
   ds=ds.rename({'lon_val':'x'})
   ds=ds.rename({'lat_val':'y'})
   ds=ds.drop('Latitude')
   ds=ds.drop('Longitude')
   print('converting to zarr')
   out=ds.to_zarr(output_dir,compute=False)
   with ProgressBar():
      results=out.compute()
   return


def set_time(ds):
   print('setting dims starting')
   time=ds.time_coverage_start
   ds=ds.assign_coords({'time': time})
   ds=ds.expand_dims('time')
   print('setting dims - dataset returned')
   return ds


if __name__ == '__main__':
   today=datetime.today()
   parser = argparse.ArgumentParser(description='Ingest GOES and create collections')
   parser.add_argument('collection_name', type = str, help = 'Enter GOES collection ie goes-ABI-L2-TPWF')
   args=parser.parse_args()
   collection_name='himawari-'+args.collection_name
   output_dir='/data/collections/himawari-'+collection_name+'/zarr'
   s3_endpoint='s3://noaa-himawari8/'+collection_name+'/'+str(today.year)+'/'+str(today.month).zfill(2)+'/'+str(today.day).zfill(2)+'/*'
   open_from_s3(s3_endpoint,output_dir)
   meta(output_dir,collection_name)
