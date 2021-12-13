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



def expand_dims(d):
   d=d.expand_dims('step')
   return d

def concat_grib(ingest_location, file_location,today,model,model_run):
   all_grb=open(file_location+'/all.grb','wb')
   for idx,f in enumerate(glob.glob(ingest_location+'/*')):
      if idx==0:
         shutil.copyfile(f,file_location+today+'_'+model+'_template.grib')
      if idx==1:
         shutil.copyfileobj(open(f,'rb'),open(file_location+today+'_'+model+'_template.grib','wb'))       
      shutil.copyfileobj(open(f,'rb'),all_grb)
   return

def create_collections(file_location,model):
   ds_dict={}
   dims_dict={}
   ds_concat_dict={}
   ds_dict={}
   ds=xr.open_dataset(file_location+'all.grb',engine='pynio')
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
      convert_to_zarr(file_location,col_ds,collection_dict,key,value)
   client.close()
   return

def convert_to_zarr(file_location,col_ds,collection_dict,key,value):
   chunk_dict={}
   for dim in col_ds.dims:
      if 'lat_' in dim:
         chunk_dict[dim]=128
      if 'lon_' in dim:
         chunk_dict[dim]=128
      if 'lv_' in dim:
         chunk_dict[dim]=1
   col_ds=col_ds.chunk(chunks=chunk_dict)
   col_ds.to_zarr(file_location+'/zarr/'+key,mode='w',compute=True)
   print(key+' converted to zarr')
   return


def create_meta(file_location,model):
   collection_list=list()
   for name in glob.glob(file_location+'/zarr/*'):
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


def remove_nulls(d):
   return {k: v for k, v in d.items() if v is not None}


def link_dirs(root_dir,file_location,today,model,model_run):
   dest=root_dir+'/collections/'+model+'/'+model_run+'z'
   try:
      os.unlink(dest+'/zarr')
      shutil.rmtree(dest+'/zarr')
   except:
      pass
   files_delete = glob.glob(dest+'/*')
   for f in files_delete:
      try:
         os.remove(f)
      except:
         print('zarr is a directory so no delete')
   files_add = glob.glob(file_location+'/*')
   for f in files_add:
      try:
         shutil.copy(f,dest+'/'+os.path.basename(f).replace(today,model_run+'z'))
         print('copying '+f)
      except:
         print(f+' not a file')
   try:
      os.symlink(file_location+'zarr',dest+'/zarr')     
   except:
      os.makedirs(dest)
      os.symlink(file_location+'zarr',dest+'/zarr')
   return



if __name__ == "__main__":
   parser = argparse.ArgumentParser(description='Ingest Model')
   parser.add_argument('model',type=str,help='Enter the model, for example gfs_100')
   parser.add_argument('model_run', type = str, help = 'Enter model run (ie. 00 for 00z)')
   parser.add_argument('root_dir', type = str, help = 'root of the data directory, ie /data')
   args=parser.parse_args()
   global model
   model = args.model
   model_run=args.model_run
   root_dir=args.root_dir
   today=datetime.today().isoformat().split('T')[0]+'T'+model_run+':00:00'
   file_location=root_dir+'/collections/'+model+'/'+today+'/'
   try:
      shutil.rmtree(file_location)
   except:
      pass
   os.makedirs(file_location)
   ingest_location=root_dir+'/'+model+'/'+today+'/'
   concat_grib(ingest_location,file_location,today,model,model_run)
   create_collections(file_location,model)
   create_meta(file_location,model)
   os.remove(file_location+'all.grb')
   link_dirs(root_dir,file_location,today,model,model_run)
