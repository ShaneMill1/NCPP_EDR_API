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
import s3fs


def expand_dims(d):
   d=d.expand_dims('step')
   return d


def create_collections(download_grib_location,zarr_output_location):
   client=Client(n_workers=12)
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
      ds=ds.expand_dims('step')
      for coord in ds.coords:
         if coord != 'step' and coord != 'latitude' and coord != 'longitude' and coord != 'valid_time' and coord != 'time':
            lv_coord_vals=ds[coord].values.astype(str).tolist()
            if isinstance(lv_coord_vals,list):
               lv_coord_val_string='_'.join(lv_coord_vals)
            else:
                lv_coord_val_string=str(float(lv_coord_vals))
            try:
               ds=ds.expand_dims(coord)
            except:
               pass
      col_name_desc='gfs_100'+'_'+"_".join(list(ds.coords))+'_'+lv_coord_val_string
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
            ds.to_zarr(fsspec.get_mapper(zarr_output_location+col_name_desc,client_kwargs={'region_name':'us-east-1'}),mode='a',append_dim='step')
            print(col_name_dict[col_name_desc]+' appended')
         except:
            print('--------WARNING----------'+col_name_dict[col_name_desc]+' FAILED TO EXPORT TO ZARR')
      else:
         ds.to_zarr(fsspec.get_mapper(zarr_output_location+col_name_desc,client_kwargs={'region_name':'us-east-1'}),mode='w')
         print(col_name_desc+' written')
      if col_name_desc not in fs_list:
         fs_list.append(col_name_desc)
   return


def create_meta(file_location,model):
   collection_list=list()
   fs=s3fs.S3FileSystem(anon=True)
   import pdb; pdb.set_trace()
   for name in fs.glob(zarr_output_location+'/*'):
      ds=xr.open_zarr(name)
      col_dict={}
      col_dict['collection_name']=name.split('/')[-1]
      dimensions=list(ds.dims.keys())
      dimension_count=len(dimensions)
      col_dict['dimension_count']=dimension_count
      col_dict['dimensions']=dimensions
      try:
         col_dict['level_type']=ds[list(ds.data_vars.keys())[0]].level_type.lower().replace(' ','_')
      except:
         col_dict['level_type']='' 
      for dim in dimensions:
         if 'forecast_time' in dim:
            ftime_list=list()
            initial_time=ds[list(ds.data_vars.keys())[0]].initial_time
            year=initial_time.split(' ')[0].split('/')[2]
            month=initial_time.split(' ')[0].split('/')[0]
            day=initial_time.split(' ')[0].split('/')[1]
            hour=initial_time.split(' ')[1].split(':')[0].replace('(','')
            initial_dt=datetime(int(year), int(month), int(day), int(hour)).isoformat()
            initial_dt_np=np.datetime64(initial_dt)
            for ftime in ds[dim].values:
               ftime_final=str(initial_dt_np+ftime).split('.')[0]
               ftime_list.append(ftime_final)
            col_dict[dim]=ftime_list
         else:
            val_list=ds[dim].values.astype(str).tolist()
            col_dict[dim]=val_list
      data_var_list=list();long_name_list=list()
      for data_var in ds.data_vars:
         data_var_list.append(data_var)
         try:
            long_name_list.append(ds[data_var].attrs['long_name'])
         except:
            long_name_list.append('')
      col_dict['parameters']=data_var_list
      col_dict['long_name']=long_name_list
      combined_dims = '_'.join(dimensions)
      #if 'lv_' not in combined_dims:
      #   level_value='-'.join(map(str,ds[list(ds.data_vars.keys())[0]].level))
      #   level_type=ds[list(ds.data_vars.keys())[0]].level_type.replace(' ','_').lower()
      #   col_dict['lv_'+level_type]=[str(level_value)]
      collection_list.append(col_dict)
   with open(file_location+'/'+initial_dt+'_'+model+'_collection.json','w') as f:
      json.dump(collection_list,f,indent=2, sort_keys=True) 
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
   create_meta(zarr_output_location,model)
