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
   remote_latest_day=fs.glob(remote_latest_year+'/*')[-1]
   remote_latest_hour=fs.glob(remote_latest_day+'/*')[-1]
   remote_hr_contents=fs.glob(remote_latest_hour+'/*')
   fileset=list()
   if len(remote_hr_contents) == 1:
      for hr in fs.glob(remote_latest_day+'/*')[-10:]:
         remote_hr_get=fs.glob(hr+'/*')[0]
         fs_file=fs.open(remote_hr_get)
         fileset.append(fs_file)
   else:
      for r in remote_hr_contents[-10:]:
         fs_file=fs.open(r)
         fileset.append(fs_file)
   print('open netcdf files into xarray dataset')
   ds=xr.open_mfdataset(fileset,concat_dim='time',preprocess=set_time)
   goes_imager_projection=ds.goes_imager_projection
   ds=move_data_vars(ds)
   client=Client()
   ds=process_dataset(ds,goes_imager_projection)
   out=ds.to_zarr(output_dir,compute=False)
   with ProgressBar():
      results=out.compute()
   return


def process_dataset(ds,goes_imager_projection):
   sat_height=goes_imager_projection.attrs['perspective_point_height'][0]
   new_proj_attrs={}
   for key, value in goes_imager_projection.attrs.items():
      if type(value).__module__=='numpy':
         new_proj_attrs.update({key: value[0]})
      else:
         new_proj_attrs.update({key: value})
   cc=CRS.from_cf(new_proj_attrs)
   ds.coords["x"] = ds.x
   ds.coords["y"] = ds.y
   ds.coords["goes_imager_projection"] = goes_imager_projection
   ds.coords["time"] = ds.time
   ds=ds.rio.write_crs(cc, inplace=True)
   ds=ds.assign_coords({'x':ds.x.values * sat_height})
   ds=ds.assign_coords({'y':ds.y.values * sat_height})
   print('reprojecting dataset to epsg:4326')
   final_ds=ds.rio.reproject('EPSG:4326')
   print('dataset reprojected')
   return final_ds


def move_data_vars(ds):
   for data_var in ds.data_vars:
      if 'x' in ds[data_var].dims and 'y' in ds[data_var].dims and 'time' in ds[data_var].dims:
         print(data_var+' kept as a data variable')
         try:
            del ds[data_var].attrs['grid_mapping']
         except:
            print('did not need to delete grid_mapping')
      else:
         #ds=ds.assign_coords({data_var: ds[data_var]})
         ds=ds.drop(data_var)
   return ds


def set_time(ds):
   print('setting dims starting')
   time=ds.time_coverage_start
   try:
      ds=ds.drop_dims('time')
   except:
      pass
   ds=ds.assign_coords({'time': time})
   ds=ds.expand_dims('time')
   print('setting dims - dataset returned')
   return ds


if __name__ == '__main__':
   today=datetime.today()
   parser = argparse.ArgumentParser(description='Ingest GOES and create collections')
   parser.add_argument('collection_name', type = str, help = 'Enter GOES collection ie goes-ABI-L2-TPWF')
   args=parser.parse_args()
   collection_name='goes-'+args.collection_name
   output_dir='/data/collections/'+collection_name+'/zarr'
   if os.path.exists(output_dir) and os.path.isdir(output_dir):
      shutil.rmtree(output_dir)
   meta_location='/data/collections/'+collection_name+'/'
   s3_name=collection_name.replace('goes-','')
   s3_endpoint='s3://noaa-goes16/'+s3_name
   open_from_s3(s3_endpoint,output_dir)
   meta(output_dir,collection_name)
