#!/opt/conda/envs/env/bin/python
# =================================================================
#
# Authors: Shane Mill <shane.mill@noaa.gov>
#
# Copyright (c) 2020 Shane Mill - National Weather Service
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import hdf5_to_xarray_module as H2X
import xarray as xr
import json
import glob
import os
import multiprocessing
import uuid
import shutil
import argparse
from datetime import datetime


def ingest_HDF5(directory,output_dir,ingest_config_fn):
   hdf_files=sorted(glob.glob(directory+'/*.HDF5'))
   cpus = multiprocessing.cpu_count()
   max_pool_size = 6
   pool = multiprocessing.Pool(cpus if cpus < max_pool_size else max_pool_size)
   for h in hdf_files:
      pool.apply_async(zarr_tmp, args=(h,ingest_config_fn))
      #zarr_tmp(h,ingest_config_fn)
   pool.close()
   pool.join()
   return 


def zarr_tmp(h,ingest_config_fn):
   print(h)
   ds = H2X.get_xarray_dataset_from_hdf5_file(h,ingest_config_fn)
   uid=str(uuid.uuid4())
   ds=ds.chunk({'lat':1000,'lon':1000})
   time_name=str(ds['time'].values[0])
   attrs=ds.attrs
   new_attrs = {str(k): str(v).replace("b'",'') for k,v in attrs.items()}
   new_attrs = {str(k): str(v).replace("'",'') for k,v in new_attrs.items()}
   ds.attrs=new_attrs
   ds.to_zarr(output_dir+'/zarr_tmp/zarr_store'+str(time_name),mode='w',compute=True)
   return


def concat_to_zarr(output_dir):
   dim_list=list()
   z_stores=sorted(glob.glob(output_dir+'/zarr_tmp/zarr_store*'))
   ds_list=list()
   dvar_list=list()
   for z in z_stores:
      print(z)
      ds=xr.open_zarr(z,decode_times=False,mask_and_scale=False)
      ds.compute()
      ds_list.append(ds)
   print('concat on time dimension')
   ds_concat=xr.concat(ds_list,'time')
   collection_meta={}
   dim_info_list=list()
   dim_info_dict={key:[] for key in ds.dims}
   for dims in ds_concat.dims:
      dim_list.append(dims)
      ftime_list=list()
      attr=getattr(ds_concat,dims)
      a_value=list()
      if 'time' in dims:
         for a in attr.values:
            epoch_time=a
            utc_time=datetime.utcfromtimestamp(a).isoformat()
            a_value.append(utc_time)
      else:
         for a in attr.values:
            try:
               b=a.replace(',','')
            except:
               b=str(a)
            a_value.append(b)
      dim_info_dict.update({dims: a_value})
   for dvars in ds_concat.data_vars:
      dvar_list.append(dvars)
   collection_meta={"collection_name": 'nasa_imerg_Grid',\
                          "dimension_count": len(ds_concat.dims),\
                          "dimensions": dim_list,\
                          "grid_type": '',\
                          "level_type": '',\
                          "long_name": dvar_list,\
                          "parameters": dvar_list}
   collection_meta=[{**collection_meta, **dim_info_dict}]
   with open(output_dir+'/latest_nasa_imerg'+'_collection.json','w') as f: 
      json.dump(collection_meta, f, indent=4)   
   ds_concat.to_zarr(output_dir+'/zarr/nasa_imerg_Grid',mode='w',compute=True)
   return



if __name__ == "__main__":
   dataset_name='Grid'
   #input_dir='/mnt/data-api/nasa_precip_HDF5/'
   #output_dir='/mnt/data-api/collections/nasa_imerg/latest/'
   parser = argparse.ArgumentParser(description='Ingest config file')
   parser.add_argument('ingest_config_fn', type = str, help = '')
   args=parser.parse_args()
   ingest_config_fn=args.ingest_config_fn
   input_dir='/home/smill/NASA_DATA/3IMERG'
   output_dir='./collections'
   ingest_HDF5(input_dir,output_dir,ingest_config_fn)
   concat_to_zarr(output_dir)
