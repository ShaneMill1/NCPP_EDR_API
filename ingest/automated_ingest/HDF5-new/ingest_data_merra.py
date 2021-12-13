import hdf5_to_xarray_module as H2X
import xarray as xr
import glob
import os
import multiprocessing
import uuid
import shutil
import argparse
import json
from datetime import datetime, timedelta

def ingest_HDF5(directory,output_dir,ingest_config_fn):
   h=directory
   zarr_tmp(h,ingest_config_fn)
   return 


def zarr_tmp(h,ingest_config_fn):
   print(h)
   ds = H2X.get_xarray_dataset_from_hdf5_file(h,ingest_config_fn)
   attrs=ds.attrs
   new_attrs = {str(k): str(v).replace("b'",'') for k,v in attrs.items()}
   new_attrs = {str(k): str(v).replace("'",'') for k,v in new_attrs.items()}
   ds.attrs=new_attrs
   ds['time'].attrs['CLASS']='DIMENSION_SCALE'
   ds['time'].attrs['NAME']='time'
   ds['time'].attrs['long_name']='time'
   ds['time'].attrs['units']='minutes since 2020-01-01 00:30:00'
   for d in ds:
      ds[d].attrs["_FillValue"]=-9999
      ds[d].attrs["missing_value"]=-9999
      d_attrs=ds[d].attrs
      d_new_attrs = {str(k): str(v).replace("b'",'') for k,v in d_attrs.items()}
      d_new_attrs = {str(k): str(v).replace("'",'') for k,v in d_new_attrs.items()}
      ds[d].attrs=d_new_attrs
      ds[d].values=ds[d].values.astype(int)
   ds.to_zarr(output_dir+'/zarr/nasa_merra_all',mode='w',compute=True)
   collection_meta={}
   dim_info_list=list()
   dim_info_dict={key:[] for key in ds.dims}
   dim_list=list();dvar_list=list()
   for dims in ds.dims:
      dim_list.append(dims)
      attr=getattr(ds,dims)
      a_value=list()
      if 'time' in dims:
         begin_date=ds['time'].begin_date
         begin_time=ds['time'].begin_time
         begin_date=begin_date.replace('[','')
         begin_date=begin_date.replace(']','')
         begin_time=begin_time.replace('[','')
         begin_time=begin_time.replace(']','')
         #values are minutes
         my_date=datetime.strptime(begin_date,"%Y%m%d")
         for a in attr.values:
            b=(my_date+timedelta(minutes=int(a))).isoformat()
            a_value.append(b)
      else: 
         for a in attr.values:
            try:
               b=a.replace(',','')
            except:
               b=str(a)
            a_value.append(b)
      dim_info_dict.update({dims: a_value})
   for dvars in ds.data_vars:
      dvar_list.append(dvars)
   collection_meta={"collection_name": 'nasa_merra_'+dataset_name,\
                          "dimension_count": len(ds.dims),\
                          "dimensions": dim_list,\
                          "grid_type": '',\
                          "level_type": '',\
                          "long_name": dvar_list,\
                          "parameters": dvar_list}
   collection_meta=[{**collection_meta, **dim_info_dict}]
   with open(output_dir+'/latest_nasa_merra'+'_collection.json','w') as f:
      json.dump(collection_meta, f, indent=4)   
   return

if __name__ == "__main__":
   dataset_name='all'
   parser = argparse.ArgumentParser(description='Ingest config file')
   parser.add_argument('ingest_config_fn', type = str, help = '')
   args=parser.parse_args()
   ingest_config_fn=args.ingest_config_fn
   input_dir='/data/nasa_merra/MERRA2_400.tavg1_2d_slv_Nx.20200101.nc4'
   output_dir='/data/collections/nasa_merra'
   ingest_HDF5(input_dir,output_dir,ingest_config_fn)
