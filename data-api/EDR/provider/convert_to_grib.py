import numpy as np
import os
from pathlib import Path
import pygrib
import rasterio
import re
import shutil
import xarray as xr


def create_grib(ds_dict,grib_template,level_meta,uuid,dir_root,write_values):
   data_vars=ds_dict['data_vars']
   coords=ds_dict['coords']
   dims=ds_dict['dims']
   coord_dict={}
   r_list=list();lv_list=list()
   for c in coords:
      '''
      if 'lv_DBLL' in c:
         lv_dbll_coords=ds_dict['coords'][c]['data']
         dkey=ds_dict['data_vars'].keys()
         param=[*dkey][0]
         r1_values=ds_dict['data_vars'][param]['attrs'][c+'_l0']
         r2_values=ds_dict['data_vars'][param]['attrs'][c+'_l1']
         for idx,l in enumerate(lv_dbll_coords):         
            r1=r1_values[idx]
            r2=r2_values[idx]
            ra=str(r1)+'-'+str(r2)
            r_list.append(ra)
         coord_name=c
         coord_data=r_list
         coord_dict.update({coord_name: coord_data}) 
      else:
      '''
      coord_name=c
      coord_data=coords[c]['data']
      coord_dict.update({coord_name: coord_data})
   msg_clone_list=list();data_values_dict={}
   grib=pygrib.open(grib_template)
   for d in data_vars.keys():
      print(d)
      data_values=data_vars[d]['data']
      data_values_dict.update({d:data_values})
      level_type=data_vars[d]['attrs']['level_type'].lower()
      lt=level_type
      level_type=level_type.replace('(','')
      level_type=level_type.replace(')','')
      level_type=level_type.split(' ')
      param_template=data_vars[d]['attrs']['parameter_template_discipline_category_number']
      long_name=data_vars[d]['attrs']['long_name']
      discipline=param_template[1]
      parameterCategory=param_template[2]
      parameterNumber=param_template[3]
      #msg_clone=grib.read(1)[0]
      for msg in grib:
         level_og=msg.nameOfFirstFixedSurface.lower()
         lt=re.sub(r" ?\([^)]+\)", "", lt)
         if level_og==lt:
            msg_clone=msg
            msg_clone['discipline']=discipline
            msg_clone['parameterCategory']=parameterCategory
            msg_clone['parameterNumber']=parameterNumber
            for lv_search in ds_dict['coords']:
               if 'lv_' in lv_search:
                  if isinstance(ds_dict['coords'][lv_search]['data'],list):
                     lev_val=ds_dict['coords'][lv_search]['data'][0]
                  else:
                     lev_val=ds_dict['coords'][lv_search]['data']
                  if '-' in str(lev_val):
                     first_surf=float(lev_val.split('-')[0])
                     second_surf=float(lev_val.split('-')[1])
                     msg_clone['scaledValueOfFirstFixedSurface']=first_surf
                     msg_clone['scaledValueOfSecondFixedSurface']=second_surf
            break
      msg_clone_list.append(msg_clone)
   output=open(dir_root+'/output1-'+uuid+'.grb',"wb")
   step_list_new=list()
   for index,data_var_message in enumerate(msg_clone_list):
      parameterName=data_var_message.parameterName
      forecast_time_key=None;lv_key=None
      for c in coord_dict:
         if 'forecast_time' in c:
            forecast_time_key=c
         if 'lv_' in c:
            lv_key=c
         if 'lat' in c:
            lat_key=c
         if 'lon' in c:
            lon_key=c
      if forecast_time_key is None and lv_key is not None:
         lv_list=coord_dict[lv_key]
         print('option 1')
         for idx,s in enumerate(lv_list):
            lat_values=coord_dict[lat_key]
            lon_values=coord_dict[lon_key]
            data_var_message.latitudeOfFirstGridPointInDegrees=min(lat_values)
            data_var_message.latitudeOfLastGridPointInDegrees=max(lat_values)
            data_var_message.longitudeOfFirstGridPointInDegrees=min(lon_values)
            data_var_message.longitudeOfLastGridPointInDegrees=max(lon_values)
            data_var_message.level=lv_list[idx]
            missing=data_var_message.missingValue
            msg=data_var_message.tostring()
            output.write(msg)
      if forecast_time_key is not None and lv_key is None:
         step_list=coord_dict[forecast_time_key]
         print('option 2')
         if not isinstance(step_list,list):
            step_list=[step_list]
         for idx,s in enumerate(step_list):
            lat_values=coord_dict[lat_key]
            lon_values=coord_dict[lon_key]
            data_var_message.latitudeOfFirstGridPointInDegrees=min(lat_values)
            data_var_message.latitudeOfLastGridPointInDegrees=max(lat_values)
            data_var_message.longitudeOfFirstGridPointInDegrees=min(lon_values)
            data_var_message.longitudeOfLastGridPointInDegrees=max(lon_values)
            step_list_new.append(int(s.total_seconds()/3600))
            data_var_message.forecastTime=step_list_new[idx]
            missing=data_var_message.missingValue
            msg=data_var_message.tostring()
            output.write(msg)
            print('done setting '+str(idx))
      elif forecast_time_key is not None and lv_key is not None:
         step_list=coord_dict[forecast_time_key]
         print('option 3')
         if type(coord_dict[lv_key])==list:
            lv_list=coord_dict[lv_key]
         else:
            lv_list=[coord_dict[lv_key]]
         for l_index,l in enumerate(lv_list):
            if data_var_message.typeOfLevel=="depthBelowLandLayer":
               data_var_message.bottomLevel=float(lv_list[l_index].split('-')[1])*100
               data_var_message.topLevel=float(lv_list[l_index].split('-')[0])*100
               data_var_message.scaleFactorOfSecondFixedSurface = 2
               data_var_message.scaleFactorOfFirstFixedSurface = 2
            else:
               try:
                  data_var_message.level=lv_list[l_index]
               except:
                  print('skipping the level set, this message does not have a level attribute')
            print(data_var_message)
            if not isinstance(step_list,list):
               step_list=[step_list]
            for idx,s in enumerate(step_list):
               lat_values=coord_dict[lat_key]
               lon_values=coord_dict[lon_key]
               data_var_message.latitudeOfFirstGridPointInDegrees=min(lat_values)
               data_var_message.latitudeOfLastGridPointInDegrees=max(lat_values)
               data_var_message.longitudeOfFirstGridPointInDegrees=min(lon_values)
               data_var_message.longitudeOfLastGridPointInDegrees=max(lon_values)
               step_list_new.append(int(s.total_seconds()/3600))
               data_var_message.forecastTime=step_list_new[idx]
               missing=data_var_message.missingValue
               msg=data_var_message.tostring()
               output.write(msg)        
               print('done setting '+str(idx))
      else:
         print('did not work')
   output.close()
   missing=-9999
   if write_values==True:
      set_values(data_values_dict,missing,uuid,dir_root)
   if write_values == False:
      shutil.copy(dir_root+'/output1-'+uuid+'.grb', dir_root+'/output-'+uuid+'.grb')
   return


def set_values(data_values_dict,missing,uuid,dir_root):
   new_list=list();tags=list();pds_temp=list();grib_ids=list()
   my_file = Path(dir_root+'/output-'+uuid+'.grb')
   if my_file.exists():
      os.remove(myfile)  
   with rasterio.open(dir_root+'/output1-'+uuid+'.grb', 'r', driver='GRIB') as src:   
      og_transform=src._transform
      c=1
      out_meta = src.meta.copy()
      count=src.count
      while c<=count:
         tags.append(src.tags(c))
         c=c+1
   nparray_list=list()
   for v in data_values_dict:
      narray=np.array(data_values_dict[v])
      nparray_list.append(narray)
   nparray=np.vstack(nparray_list)
   if len(nparray.shape)>3:
      nparray=nparray.swapaxes(0,1)
   else:
      pass

   try:
      if len(nparray.shape)>2:
         nparray=nparray.reshape(nparray.shape[0]*nparray.shape[1],nparray.shape[2],nparray.shape[3])
      else:
         pass
   except:
      pass
   if len(nparray.shape)>2:
      out_meta.update({"driver": "GRIB","height": nparray.shape[1],"width": nparray.shape[2],"count":count})
   else:
      out_meta.update({"driver": "GRIB","height": nparray.shape[0],"width": nparray.shape[1],"count":count})
   with rasterio.open(dir_root+'/output-'+uuid+'.grb', 'w', **out_meta) as dst:
      print('metadata added')
   nparray=np.nan_to_num(nparray,missing)
   with rasterio.open(dir_root+'/output-'+uuid+'.grb', 'r+', driver='GRIB',DATA_ENCODING='SIMPLE_PACKING') as dsv:
      try:
         dsv.write(nparray)
      except:
         print('could not write values')
   with rasterio.open(dir_root+'/output-'+uuid+'.grb', 'r+', driver='GRIB',DATA_ENCODING='SIMPLE_PACKING') as ds:
      for idx,t in enumerate(tags,start=0):
         ds.update_tags(idx+1,GRIB_PDS_TEMPLATE_NUMBERS=t['GRIB_PDS_TEMPLATE_NUMBERS'])
         ds.update_tags(idx+1,GRIB_IDS=t['GRIB_IDS'])
         ds.update_tags(idx+1,GRIB_DISCIPLINE=t['GRIB_DISCIPLINE'])
   try:
      os.remove(dir_root+'/output1-'+uuid+'.grb')
   except:
      pass

   return
