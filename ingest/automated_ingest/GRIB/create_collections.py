#!/opt/conda/envs/env/bin/python
# =================================================================
#
# Authors: Shane Mill <shane.mill@noaa.gov>
#
# Copyright (c) 2019 Shane Mill - National Weather Service
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
import argparse
from collections import OrderedDict
import model_download_ingest
import math
import canada_ingest
import glob
import json
import os
import multiprocessing
import numpy as np
import pandas as pd
import re
import shutil
import xarray as xr
import subprocess
import dask
import distributed
import zarr
from dask.distributed import Client, progress
from datetime import datetime, timedelta

def download_data(model,cycle,ingest_path):
   #downloads model data into ./data_ingest directory
   if 'gem' in model:
      ds=canada_ingest.model_ingest(cycle,model,ingest_path)
   if 'nbm' in model:
      pass
      #print('copying nbm data to data-api for '+str(cycle)+'z run.')
      #nbm_dir='/mnt/nbm/nbm_view_tmpdata/raw/blendv32/co/grib2/'
      #date=datetime.today().strftime('%Y%m%d')
      #model_run=cycle[:-1]
      #cp_dir=nbm_dir+'/'+str(date)+'/'+str(model_run)+'/'
      #dest_dir='/mnt/data-api/nbm_v32/'+str(cycle)+'z'
      #destination=shutil.copytree(cp_dir,dest_dir)
      #print(destination) 
   else:
      ds=model_download_ingest.model_ingest(cycle,model,ingest_path)
   return


def create_all_grb(model,cycle,ingest_path):
   #concatenates the grib files together for one model grib file containing 0z/06z/12z/18z runs
   path=sorted(glob.glob(ingest_path+'/'+model+'/'+cycle+'/*'))
   if not os.path.exists(ingest_path+'/collections/'+model+'/'+cycle):
      os.makedirs(ingest_path+'/collections/'+model+'/'+cycle,exist_ok=True)
   model_dir=ingest_path+'/collections/'+model+'/'+cycle+'/'
   if os.path.exists(model_dir+cycle+'_'+model+'.grb'):
         os.remove(model_dir+cycle+'_'+model+'.grb')
   all_grb=open(model_dir+cycle+'_'+model+'.grb','wb')
   for f in path:
      shutil.copyfileobj(open(f,'rb'),all_grb)
   for f in path[0:1]:
      shutil.copyfile(f,model_dir+cycle+'_'+model+'_template.grib')
   all_grb.close()
   return


def extract_crs(INPUT):
   process = subprocess.run(['gdalsrsinfo', INPUT], stdout=subprocess.PIPE)
   result=str(process.stdout)
   result=result.split(":")
   crs=result[1]
   crs=crs.replace('\\n','')
   crs=crs.replace('OGC WKT2','')
   return crs


def create_model_csv(model,cycle,ingest_path):
   #opens concatenated grib as an xarray dataset using pynio. 
   #grabs the unique parameter ID, long name, level type, and dimensions
   #creates a record of each individual ID and its associated dimensions
   #also creates a csv that will act as a map between the dimension names and their values.
   #See GFS.csv and GFS_dim_info.csv as examples.
   model_dir=ingest_path+'/collections/'+model+'/'+cycle+'/'
   path=model_dir+cycle+'_'+model+'.grb'
   try:
      crs=extract_crs(path)
   except:
      crs='crs_not_extracted'
   print(crs)
   ds=xr.open_dataset(path,engine='pynio')
   all_ds=ds
   grid_type_list=list()
   long_name_list=list()
   units_list=list()
   dim_dict_list=list()
   overall_list=list()
   for d in ds.data_vars:
      if hasattr(ds.data_vars[d], 'long_name') and hasattr(ds.data_vars[d], 'grid_type') and hasattr(ds.data_vars[d], 'units') and hasattr(ds.data_vars[d], 'dims'):
         grid_type_list.append(ds.data_vars[d].grid_type)
         long_name_list.append(ds.data_vars[d].long_name)
         units_list.append(ds.data_vars[d].units)
         dim_dict={key:[] for key in ds.data_vars[d].dims}
         for f in ds.data_vars[d].dims:
            ds_vars=getattr(ds.data_vars[d],f)
            dim_dict[f].append(f)
         combined_dims = '\t'.join(ds.data_vars[d].dims)
         if 'lv_' not in combined_dims:
            level_type=ds[d].level_type.replace(' ','_').lower()
            dim_dict['lv_'+level_type]=['lv_'+level_type]
         dim_dict.update({'id': ds.data_vars[d]._name})
         dim_dict.update({'long_name': ds.data_vars[d].long_name})
         try:
            dim_dict.update({'level_type': ds.data_vars[d].level_type})
         except:
            dim_dict.update({'level_type': 'level_type_unavailable'})
         dim_dict.update({'grid_type': crs})
         overall_list.append(dim_dict)
   df=pd.DataFrame.from_dict(overall_list)
   cols=df.columns.tolist()
   cols.insert(0,cols.pop(cols.index('id')))
   cols.insert(1,cols.pop(cols.index('long_name')))
   cols.insert(2,cols.pop(cols.index('level_type')))
   cols.insert(3,cols.pop(cols.index('grid_type')))
   df=df.reindex(columns=cols)
   df.to_csv(model_dir+cycle+'_'+model+'.csv', sep=',', na_rep=' ', index=True)
   dim_info_list=list()
   dim_info_dict={key:[] for key in ds.dims}
   initial_time=ds[dim_dict['id']].initial_time
   initial_time=initial_time.replace('(','')
   initial_time=initial_time.replace(')','')
   initial_time=initial_time.replace('/','-')
   month=initial_time[0:2]
   day=initial_time[3:5]
   year=initial_time[6:10]
   time=initial_time[11:16]
   initial_time=np.datetime64(year+'-'+month+'-'+day+' '+time)
   for z in ds.dims:
      attr=getattr(ds,z)
      a_value=list()
      if 'forecast_time' in z:
         td_values=attr.values
         valid_time=[]
         for d in td_values:
            t=initial_time+d
            t=str(t)
            t=t[:-10]
            valid_time.append(t)
         coord_dict={}
         coord_dict[z]=valid_time
         ds=ds.assign_coords(coord_dict)
      attr=getattr(ds,z)
      dv_list=list()
      for a in attr.values:
         a_value.append(a)
      dim_info_dict.update({z: a_value})
   for d in ds.data_vars:
      if 'lv' in d and '_l0' in d:
         #work for range
         end_value=list()
         index=d[:-3]
         r0=ds[index].values
         r1=ds[index+'_l0'].values
         r2=ds[index+'_l1'].values
         for r in r0:
            value=str(r1[r])+'-'+str(r2[r])
            end_value.append(value)
         di=d[:-3]
         dim_info_dict.update({di: end_value})
      combined_dims_2 = '\t'.join(ds.data_vars[d].dims)
      if 'lv_' not in combined_dims_2:
         level_type=ds[d].level_type.replace(' ','_').lower()
         dim_info_dict.update({'lv_'+level_type: ds[d].level.tolist()})
      else:
         pass
      dim_info_list.append(dim_info_dict)
   ef=pd.DataFrame.from_dict(dim_info_list)
   ef = ef.iloc[0]
   ef.to_csv(model_dir+cycle+'_'+model+'_dim_info.csv', sep=',', na_rep=' ', index=True) 
   return all_ds


def group_parameters(model,cycle,ingest_path):
   #This will use the GFS.csv to group together parameters that have the same exact dimensions.
   model_dir=ingest_path+'/collections/'+model+'/'+cycle+'/'
   csv=cycle+'_'+model+'.csv'
   element_list=list()
   element_all=list()
   df=pd.read_csv(model_dir+csv)
   lookup_df=df[['id','long_name','level_type','grid_type']]  
   #when looking at the groupby columns, please look at the "col" list or you may be misled
   col=list(df.columns)
   ef=df.groupby(col[3:])['id'].apply(list).reset_index(name='parameters')
   ef['long_name']=ef['parameters']
   ln_lookup=pd.Series(lookup_df.long_name.values,index=lookup_df.id).to_dict()
   ef=ef.assign(long_name=[[ln_lookup[k] for k in row if ln_lookup.get(k)] for row in ef.long_name])
   ef.to_csv(model_dir+csv[:-4]+'_grouping.csv', sep=',', index=False)
   return


def create_collections(model,cycle,ingest_path):
   #this creates the end result in csv and json format where we have each "collection" and the associated parameter IDs and additional 
   #dimension info.
   model_dir=ingest_path+'/collections/'+model+'/'+cycle+'/'
   group_file=cycle+'_'+model+'_grouping.csv'
   dim_file=cycle+'_'+model+'_dim_info.csv'
   df=pd.read_csv(model_dir+group_file)
   dim_cols=['dim_name','dim_val']
   dim_df=pd.read_csv(model_dir+dim_file,names=dim_cols)
   dim_name=dim_df['dim_name']
   dim_df['dim_val']=dim_df['dim_val'].str.replace('\n','')
   dim_df['dim_val']=dim_df['dim_val'].str.replace('[','')
   dim_df['dim_val']=dim_df['dim_val'].str.replace(']','')
   dim_df['dim_val']=dim_df['dim_val'].str.replace(',','')
   dim_df['dim_val']=dim_df['dim_val'].str.split()
   dim_value=dim_df['dim_val']
   dim_lookup=pd.Series(dim_df.dim_val.values,index=dim_df.dim_name).to_dict()
   df_2=pd.read_csv(model_dir+group_file)
   d_list=list()
   for d in dim_name:
      if type(d)!=str and math.isnan(d)==True:
         d='unknown_key'
      else:
         df_2[d]=df_2[d].astype('str')
         df_2[d]=df_2[d].str.replace("]","")
         df_2[d]=df_2[d].str.replace("[","")
         df_2[d]=df_2[d].str.replace("'","")
         df_2[d]=df_2[d].str.replace("'","")
         df_2[d]=df_2[d].astype('str')
         df_2[d]=df_2[d].map(dim_lookup)
         d_list.append(d)
   dval_df=df_2[d_list]
   cols=list(df.columns) 
   cols.insert(0, cols.pop(cols.index('parameters')))
   cols.insert(1, cols.pop(cols.index('long_name')))
   cols.insert(2, cols.pop(cols.index('level_type')))
   cols.insert(3, cols.pop(cols.index('grid_type')))
   df=df.reindex(columns=cols)
   df['dimensions']=df[cols[4:]].apply(lambda row: ','.join(row.values.astype(str)), axis=1)
   cols=list(df.columns)
   cols.insert(3, cols.pop(cols.index('dimensions')))
   cols.insert(4, cols.pop(cols.index('grid_type')))
   df=df.reindex(columns=cols)
   df = df[['parameters','long_name','level_type','dimensions','grid_type']]
   df['dimensions']=df['dimensions'].str.replace(' ,','')
   df['dimensions']=df['dimensions'].str.replace(', ','')
   df['dimensions']=df['dimensions'].str.replace("'",'')
   df['dimensions']=df['dimensions'].str.replace(']','')
   df['dimensions']=df['dimensions'].str.replace('[','')
   df['parameters']=df['parameters'].str.replace(']','')
   df['parameters']=df['parameters'].str.replace('[','')
   df['parameters']=df['parameters'].str.replace("'",'')
   df['long_name']=df['long_name'].str.replace("'",'')
   df['long_name']=df['long_name'].str.replace(']','')
   df['long_name']=df['long_name'].str.replace('[','')
   for id1,dim in enumerate(df['dimensions']):
      dm=dim.split(',')
      for idx,di in enumerate(dm):
         if 'lv_' in di:
            index=idx
      lv_dm=dm[index]
      lv_dm_split=lv_dm.split('_')
      if lv_dm_split[1].islower():
         dim=dim.replace(','+lv_dm,'')
         df['dimensions'][id1]=dim
   df['collection_name']=df['dimensions'].str.replace(',','_')
   df['parameters']=df.parameters.map(lambda x: [i.strip() for i in x.split(',')])
   df['dimensions']=df.dimensions.map(lambda x: [i.strip() for i in x.split(',')])
   df['long_name']=df.long_name.map(lambda x: [i.strip() for i in x.split(',')])
   df['level_type']=df.level_type.map(lambda x: [i.strip() for i in x.split(',')])
   df['level_type']=df.level_type.astype(str)
   df['level_type']=df['level_type'].str.replace(']','')
   df['level_type']=df['level_type'].str.replace('[','')
   df['level_type']=df['level_type'].str.replace('(','')
   df['level_type']=df['level_type'].str.replace(')','')
   df['level_type']=df['level_type'].str.replace(' ','_')
   df['level_type']=df['level_type'].str.replace("'","")
   df['grid_type']=df.grid_type.map(lambda x: [i.strip() for i in x.split(',')])
   df['grid_type']=df.grid_type.astype(str)
   df['grid_type']=df['grid_type'].str.replace(']','')
   df['grid_type']=df['grid_type'].str.replace('[','')
   df['grid_type']=df['grid_type'].str.replace('(','')
   df['grid_type']=df['grid_type'].str.replace(')','')
   df['grid_type']=df['grid_type'].str.replace(' ','_')
   df['grid_type']=df['grid_type'].str.replace("'","")
   df['collection_name']=model+'_'+df['collection_name']+'_'+df['level_type']
   print(df['collection_name'])
   df['dimension_count']=df.dimensions.apply(len)
   df=pd.concat([df,dval_df],axis=1)
   df=df.sort_values(by=['dimension_count'])
   cols=list(df.columns)
   cols.insert(0, cols.pop(cols.index('collection_name')))
   cols.insert(1, cols.pop(cols.index('parameters')))
   cols.insert(2, cols.pop(cols.index('long_name')))
   cols.insert(3, cols.pop(cols.index('level_type')))
   cols.insert(4, cols.pop(cols.index('dimension_count')))
   cols.insert(5, cols.pop(cols.index('dimensions')))
   cols.insert(6, cols.pop(cols.index('grid_type')))
   df.to_csv(model_dir+group_file[:-13]+'_collection.csv', sep=',', index=False)
   j=df.to_json(orient='records')
   with open(model_dir+group_file[:-13]+'_collection.json','w') as f: 
      res = json.loads(j,object_hook=remove_nulls)
      json.dump(res,f,indent=2, sort_keys=True)
   return 


def remove_nulls(d):
   return {k: v for k, v in d.items() if v is not None}

def to_zarr_one(ds,dl,ingest_path,model):
   ingest=ingest_path+'/zarr/nbm_v32_all'
   try:
      dsd=ds[dl].to_dataset()
      dsd.to_zarr(ingest,mode='a',compute=True)
      print('success: '+dl+' converted to zarr')
   except:
      print('warning: '+dl+' failed to convert to zarr')
   return 'converted to zarr'

def one_collection(model,cycle,ingest_path,all_ds):
   ingest_path=ingest_path+'/collections/'+model+'/'+cycle
   one_dict={};xgrid_list=list();ygrid_list=list();forecast_time_list=list();lv_list=list()
   probability_list=list();long_name_list=list();parameters_list=list()
   try:
      shutil.rmtree(ingest_path+'/zarr/')
   except:
      pass
   with open(ingest_path+'/'+cycle+'_'+model+'_collection.json','r') as json_f:
      f=json.load(json_f)
      for c in f:
         for dim in c['dimensions']:
            if 'xgrid' in dim:
               xgrid_key=dim
               xgrid_list=xgrid_list+c[xgrid_key]   
            if 'ygrid' in dim:
               ygrid_key=dim
               ygrid_list=ygrid_list+c[ygrid_key]
            if 'forecast_time' in dim:
               forecast_time_key=dim
               forecast_time_list=forecast_time_list+c[forecast_time_key]
            #if 'lv' in dim:
            #   lv_key=dim
            #   lv_list=lv_list+c[lv_key]
            if 'probability' in dim:
               probability_key=dim
               probability_list=probability_list+c[probability_key]
         long_name_list=long_name_list+c['long_name']
         parameters_list=parameters_list+c['parameters']
      xgrid_list = list(dict.fromkeys(xgrid_list))
      ygrid_list = list(dict.fromkeys(ygrid_list))
      forecast_time_list = list(dict.fromkeys(forecast_time_list))
      lv_list = list(dict.fromkeys(lv_list))
      probability_list = list(dict.fromkeys(probability_list))
      one_dict['dimensions']=['xgrid','ygrid','lv','forecast_time','probability'] 
      one_dict['xgrid_0']=xgrid_list
      one_dict['ygrid_0']=ygrid_list
      one_dict['forecast_time']=forecast_time_list
      one_dict['lv']=lv_list
      one_dict['probability']=probability_list
      one_dict['collection_name']='nbm_v32_all'
      one_dict['long_name']=long_name_list
      one_dict['parameters']=parameters_list
   with open(ingest_path+'/'+cycle+'_'+model+'_collection.json','w') as json_final:
      json_list=list()
      json_list=[one_dict]
      json.dump(json_list,json_final,indent=2, sort_keys=True)
   ds=all_ds
   chunk_dict={}
   d_list=list();
   for d in ds.data_vars:
      chunk_dict={}
      for dim in ds[d].dims:
         if 'forecast_time' in dim:
            ftime=dim
            chunk_dict[ftime]=1
         if 'lv' in dim:
            lv=dim
            chunk_dict[lv]=1
         if 'probability' in dim:
            probability=dim
            chunk_dict[probability]=1
         if 'xgrid' in dim:
            xgrid=dim
            chunk_dict[xgrid]=128
         if 'ygrid' in dim:
            ygrid=dim
            chunk_dict[ygrid]=128
      ds[d]=ds[d].chunk(chunks=chunk_dict)
      if 'forecast_time' in d or 'grid' in d or 'probability' in d or 'lv_' in d:
         pass
      else:
         d_list.append(d)
   print('converting to zarr')
   cpus = multiprocessing.cpu_count()
   max_pool_size = 6
   pool2 = multiprocessing.Pool(cpus if cpus < max_pool_size else max_pool_size)
   for dl in d_list:
      pool2.apply_async(to_zarr_one, args=(ds,dl,ingest_path,model))
   pool2.close()
   pool2.join()   
   return


def convert_to_zarr(model,cycle,ingest_path,all_ds):
   ingest_path_new=ingest_path+'collections/'+model+'/'+cycle
   col_json_file=ingest_path_new+'/'+cycle+'_'+model+'_collection.json'
   ds_f=ingest_path_new+cycle+'_'+model+'.grb'
   with open(col_json_file) as json_file:
      col_json = json.load(json_file)
   ds=all_ds
   lv_attr_dict={}
   np.set_printoptions(precision=1)
   for d in ds.data_vars:
      if 'lv_' in d:
         values=ds[d].values
         values=np.round(values.astype(np.float64),decimals=2)
         lv_attr_dict[d]=values.tolist()
   del d
   for d in ds.data_vars:
      for dim in ds[d].dims:
         for l in lv_attr_dict:
            if str(l[:-5]) in dim:
               ds[d].attrs[l]=lv_attr_dict[l]
   cpus = multiprocessing.cpu_count()
   max_pool_size = 3
   pool = multiprocessing.Pool(cpus if cpus < max_pool_size else max_pool_size)
   client=Client()
   for idx,c in enumerate(col_json):
      dims=c['dimensions']
      chunk_dict={}
      for d in dims:
         if 'lv' in d:
            lv_key=d
            lv_size=len(col_json[idx][lv_key])
            #lv_chunk=int(lv_size/2)
            lv_chunk=1
            chunk_dict[lv_key]=lv_chunk
         if 'lat_' in d:
            chunk_dict[d]=64
         if 'lon_' in d:
            chunk_dict[d]=64
         if 'forecast_time' in d:
            chunk_dict[d]=1
      #pool.apply_async(to_zarr_pool, args=(ds,ingest_path_new,c,chunk_dict))
      param=c['parameters']
      param=[s.replace("'","") for s in param]
      col_ds=ds[param].chunk(chunks=chunk_dict)
      to_zarr_pool(col_ds,ingest_path_new,c)
   pool.close()
   pool.join()
   print('linking to latest')
   cycle_name=cycle.split('T')[1].split(':')[0]
   if os.path.exists(ingest_path+'collections/'+model+'/'+cycle_name+'z'):
      shutil.rmtree(ingest_path+'collections/'+model+'/'+cycle_name+'z') 
   try:
      os.makedirs(ingest_path+'collections/'+model+'/'+cycle_name+'z/zarr')
   except:
      pass
   for fp in glob.glob(ingest_path_new+'/zarr/*'):
      os.symlink(fp,ingest_path+'collections/'+model+'/'+cycle_name+'z/zarr/'+os.path.basename(fp))
   for fm in glob.glob(ingest_path_new+'/*'):
      if '.grb' not in fm and 'zarr' not in fm:
         new_file=os.path.basename(fm).split(':')[-1][0:2]+'z'+os.path.basename(fm).split(':')[-1][2:] 
         shutil.copyfile(fm,ingest_path+'collections/'+model+'/'+cycle_name+'z/'+new_file)
   return


def to_zarr_pool(col_ds,ingest_path,c):
   item_list=list()
   col=c
   for cs in col.keys():
      if 'lv_' in cs:
         cs_list=cs.split('_')
         if cs_list[1].islower():
            for item in c[cs]:
               item_list.append(float(item))
            col_ds=col_ds.expand_dims({cs: item_list})
   col_ds.to_zarr(ingest_path+'/zarr/'+c['collection_name'],mode='w',compute=True)
   print(c['collection_name']+' converted to zarr')
   return


def cleanup(ingest_path):
   print("cleanup")
   os.system('find '+ingest_path+' -name "*.grb" -type f -delete')
   return


if __name__ == "__main__":

   parser = argparse.ArgumentParser(description='Ingest Model and create collections')
   parser.add_argument('model', type = str, help = 'Enter the model (ex: GFS)')
   parser.add_argument('cycle', type = str, help = 'Enter the model cycle (ex: 12z)')
   parser.add_argument('ingest_path', type = str, help = 'Enter the directory where you want the data to be stored')
   args=parser.parse_args()
   model=args.model
   cycle=args.cycle
   ingest_path=args.ingest_path
   try:
      cycle_date=datetime.now().isoformat()[0:11]
      if 'z' in cycle:
         cycle=cycle.replace('z','')
      cycle=cycle_date+cycle+':00:00'
      #download_data(model,cycle,ingest_path)
      #create_all_grb(model,cycle,ingest_path)
      all_ds=create_model_csv(model,cycle,ingest_path)
      group_parameters(model,cycle,ingest_path)
      create_collections(model,cycle,ingest_path)
   except:
      cycle_date=(datetime.now()-timedelta(days=1)).isoformat()[0:11]
      if 'z' in cycle:
         cycle=cycle.replace('z','')
      cycle=cycle_date+cycle+':00:00'
      download_data(model,cycle,ingest_path)
      create_all_grb(model,cycle,ingest_path)
      all_ds=create_model_csv(model,cycle,ingest_path)
      group_parameters(model,cycle,ingest_path)
      create_collections(model,cycle,ingest_path)
   if 'nbm' in model:
      one_collection(model,cycle,ingest_path, all_ds)
   else:
      convert_to_zarr(model,cycle,ingest_path, all_ds)
 
