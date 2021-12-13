#!/opt/conda/envs/env/bin/python
from html.parser import HTMLParser
import xarray as xr
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from dateutil import parser
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent import futures
import time
import pytz
import json
import os
import zarr
import copy
import numpy as np
import pandas as pd
import shutil
import glob
from scipy.constants import convert_temperature
from distributed import Client
import cfgrib
import subprocess
import uuid
import gc



BASE_URL_NDFD="https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/"
BASE_URL_RTMA="https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndgd/GT.rtma/"
BASE_URL_NDFD_EXP="https://tgftp.nws.noaa.gov/SL.us008001/ST.expr/DF.gr2/DC.ndfd/"
BASE_URL_LIST=[BASE_URL_RTMA,BASE_URL_NDFD,BASE_URL_NDFD_EXP]


LOCAL_BASE_FOLDER = "/data/"
LOCAL_DOWNLOAD_FOLDER = './download/'
M_WORKERS = 25
LOCATIONS = ['AR.conus','AR.guam','AR.hawaii','AR.puertori','AR.alaska', 'AR.nhemi', 'AR.npacocn', 'AR.oceanic']

NDFD_Mapping={
   'apt':'appt',
   'conhazo':'conhazo',
   'critfireo':'critfireo',
   'dryfireo':'dryfireo',
   'fret':'evp24',
   'fretdep':'evpdep24',
   'frettot':'evp168',
   'iceaccum':'iceaccum',
   'maxrh':'maxrh',
   'maxt':'maxt',
   'minrh':'minrh',
   'mint':'mint',
   'phail':'phail',
   'p24less0025':'probtsrwe24',
   'pop12':'pop12',
   'ppi':'ppi',
   'ptornado':'ptornado',
   'ptotsvrtstm':'ptotsvrtstm',
   'ptotxsvrtstm':'pxtotsvrtstm',
   'ptstmwinds':'ptstmwinds',
   'pxhail':'pxhail',
   'pxtornado':'pxtornado',
   'pxtstmwinds':'pxtstmwinds',
   'qpf':'qpf',
   'rhm':'rh',
   'snow':'snow',
   'sky': 'sky',
   'snowlevel':'snowlvl',
   'tcfrt': 'tcrain',
   'tcsst': 'tcsurge',
   'tctt': 'tctornado',
   'tcwspdabv34c': 'cumw34',
   'tcwspdabv34i': 'incw34',
   'tcwspdabv50c': 'cumw50',
   'tcwspdabv50i': 'incw50',
   'tcwspdabv64c': 'cumw64',
   'tcwspdabv64i': 'incw64',
   'tcwt': 'tcwind',
   'td': 'dew',
   'temp': 'temp',
   'waveh': 'waveh',
   'wdir': 'wdir',
   'wgust': 'wgust',
   'wspd': 'wspd',
   'wwa': 'wwa',
   'wx': 'wx',
   'prcpabv14d': 'prcpabv14d',
   'prcpabv30d': 'prcpabv30d',
   'prcpabv90d': 'prcpabv90d',
   'prcpblw14d': 'prcpblw14d',
   'prcpblw30d': 'prcpblw30d',
   'prcpblw90d': 'prcpblw90d',
   'tmpabv14d': 'tmpabv14d',
   'tmpabv30d': 'tmpabv30d',
   'tmpabv90d': 'tmpabv90d',
   'tmpblw14d': 'tmpblw14d',
   'tmpblw30d': 'tmpblw30d',
   'tmpblw90d': 'tmpblw90d',
   'precipa':'precipa',
}


NDFD_EXP_Mapping={
   'ppi':'ppi',
   'snow24e10':'snow24e10',
   'snow24e90':'snow24e90',
   'snow48e10':'snow48e10',
   'snow48e90':'snow48e90',
   'snow72e10':'snow72e10',
   'snow72e90':'snow72e90',
   'wbgt':'wbgt'
}


RTMA_Mapping = {
   'precipa': 'precipa_r',
   'tcdc': 'sky_r',
   'td': 'td_r',
   'temp': 'temp_r',
   'utd': 'utd',
   'utemp': 'utemp',
   'uwdir': 'uwdir',
   'uwspd': 'uwspd',
   'wdir': 'wdir_r',
   'wspd': 'wspd_r'
}

precision_mapping={
 'maxt':{'dtype':int},
 'mint':{'dtype':int},
 'temp':{'dtype':float,'prec':1},
 'dew':{'dtype':float,'prec':1},
 'appt':{'dtype':float,'prec':1},
 'pop12':{'dtype':int},
 'qpf':{'dtype':float,'prec':2},
 'snow':{'dtype':float,'prec':2},
 'sky':{'dtype':int},
 'rh':{'dtype':float,'prec':1},
 'wspd':{'dtype':float,'prec':1},
 'wdir':{'dtype':int},
 'waveh':{'dtype':float,'prec':1},
 'incw34':{'dtype':int},
 'incw50':{'dtype':int},
 'incw64':{'dtype':int},
 'cumw34':{'dtype':int},
 'cumw50':{'dtype':int},
 'cumw64':{'dtype':int},
 'wgust':{'dtype':float,'prec':1},
 'critfireo':{'dtype':int},
 'dryfireo':{'dtype':int},
 'conhazo':{'dtype':int},
 'ptornado':{'dtype':int},
 'phail':{'dtype':int},
 'ptstmwinds':{'dtype':int},
 'pxtornado':{'dtype':int},
 'pxhail':{'dtype':int},
 'pxtstmwinds':{'dtype':int},
 'ptotsvrtstm':{'dtype':int},
 'pxtotsvrtstm':{'dtype':int},
 'tmpabv14d':{'dtype':int},
 'tmpblw14d':{'dtype':int},
 'tmpabv30d':{'dtype':int},
 'tmpblw30d':{'dtype':int},
 'tmpabv90d':{'dtype':int},
 'tmpblw90d':{'dtype':int},
 'prcpabv14d':{'dtype':int},
 'prcpblw14d':{'dtype':int},
 'prcpabv30d':{'dtype':int},
 'prcpblw30d':{'dtype':int},
 'prcpabv90d':{'dtype':int},
 'prcpblw90d':{'dtype':int},
 'precipa_r': {'dtype':float,'prec':2},
 'sky_r':{'dtype':int},
 'td_r':{'dtype':float,'prec':1},
 'temp_r':{'dtype':float,'prec':1},
 'wdir_r':{'dtype':int},
 'wspd_r':{'dtype':float,'prec':1},
 'iceaccum':{'dtype':float,'prec':2},
 'maxrh':{'dtype':int},
 'minrh':{'dtype':int},
 'evp24':{'dtype':float,'prec':2},
 'evpdep24':{'dtype':float,'prec':2},
 'evp168':{'dtype':float,'prec':2},
 'tcsurge':{'dtype':int},
 'tcrain':{'dtype':int},
 'tcwind':{'dtype':int},
 'tctornado':{'dtype':int},
 'ppi':{'dtype':int},
 'probtsrwe24':{'dtype':int},
 'snowlvl':{'dtype':int},
 'snow24e10':{'dtype':int},
 'snow24e90':{'dtype':int},
 'snow48e10':{'dtype':int},
 'snow48e90':{'dtype':int},
 'snow72e10':{'dtype':int},
 'snow72e90':{'dtype':int},
 'wbgt':{'dtype':float,'prec':1},
 'utemp':{'dtype':float,'prec':1},
 'utd':{'dtype':float,'prec':1},
 'uwspd':{'dtype':float,'prec':1},
 'uwdir':{'dtype':int},
}


def requests_retry_session(
    retries=5,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504, 404),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def getLinks(url):
    links = []
    parser = MyHTMLParser()
    try:
       r = requests.get(url)
       parser.feed(r.text)
       links = list(parser.get_links())
       parser.reset_links()
       links.pop(0)
    except:
       pass
    return list(links)


class MyHTMLParser(HTMLParser):
    links = []
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            if attrs[0][1].find('?') == -1:
                self.links.append(attrs[0][1])
    def reset_links(self):
        self.links.clear()
    def get_links(self):
        return self.links


def downloadfile(region, period, name, folderdate):
    if not os.path.exists(LOCAL_DOWNLOAD_FOLDER +'ndfd/'+ folderdate ):
        os.makedirs(LOCAL_DOWNLOAD_FOLDER +'ndfd/'+ folderdate)
    if not os.path.exists(LOCAL_DOWNLOAD_FOLDER + 'ndfd/'+folderdate + "/" + region):
        os.makedirs(LOCAL_DOWNLOAD_FOLDER +'ndfd/'+folderdate + "/" +
                    region)
    if not os.path.exists(LOCAL_DOWNLOAD_FOLDER + 'ndfd/'+folderdate + "/" + region + period):
        os.makedirs(LOCAL_DOWNLOAD_FOLDER + 'ndfd/'+folderdate + "/" +
                    region + period)
    r = requests_retry_session().get(BASE_URL + region + period + name, stream=True)
    try:
       if 'RT.' in region + period + name:
          SEARCH_DICT=RTMA_Mapping
       else:
          if 'expr' in BASE_URL:
             SEARCH_DICT=NDFD_EXP_Mapping
          else:
             SEARCH_DICT=NDFD_Mapping
       wx_elem=name.split('.')[1]
       if wx_elem in SEARCH_DICT.keys():
          if ('puerto' in region and 'RT' in period):
             print('must skip RT for puertori because data is old')
          else:
             with open(LOCAL_DOWNLOAD_FOLDER +'ndfd/'+ folderdate + "/" +
                         region + period + name.replace('bin', 'grb'), 'wb') as f:
               print(LOCAL_DOWNLOAD_FOLDER +'ndfd/'+ folderdate + "/" + region + period + name.replace('bin', 'grb'))
               for chunk in r.iter_content(chunk_size=1024):
                  if chunk:  # filter out keep-alive new chunks
                      f.write(chunk)
       else:
          print(region+period+name+' file name not relevant to the desired weather parameters for ingest') 
    except:
       print(LOCAL_DOWNLOAD_FOLDER +'ndfd/'+ folderdate + "/" +region + period + name+ ' could not open for ingest')


def create_wx_wwa_dict(file_name):
   try:
      shutil.rmtree('wx_wwa_tables')
   except:
      pass
   try:
      os.makedirs('wx_wwa_tables')
   except:
      pass
   os.system("./degrib '"+file_name+"' -C -msg all")
   if 'wx' in file_name:
      os.system("mv Wx* wx_wwa_tables")
   if 'wwa' in file_name:
      os.system("mv WWA* wx_wwa_tables")
   wwa_wx_mapping_dict={}

   uuid_str=str(uuid.uuid4())
   os.system("./degrib "+file_name+" -I > ./grib_inventory"+uuid_str+".csv")
   mapped_ds_list=list()
   grib_inventory_df=pd.read_csv('./grib_inventory'+uuid_str+'.csv', sep=",")
   os.remove('./grib_inventory'+uuid_str+'.csv')
   valid_time_df=grib_inventory_df[grib_inventory_df.columns[6]]
   del grib_inventory_df
   gc.collect()
   valid_time_list=list()
   for vt in valid_time_df:
      valid_split=vt[1:].split(' ')
      valid_day=valid_split[0]
      valid_time=valid_split[1]
      year=valid_day.split('/')[2]
      month=valid_day.split('/')[0]
      day=valid_day.split('/')[1]
      time=valid_time+':00'
      iso_time=year+'-'+month+'-'+day+'T'+time
      valid_time_list.append(iso_time)
   for idx, wwa_info in enumerate(glob.glob('wx_wwa_tables/*')):
      df=pd.read_csv(wwa_info, sep="|", header=None)
      df=df.loc[df[0] == 'PDS-S2 ']
      wwa_wx_dict={}
      for idd,i in enumerate(df[2][1:]):
         wwa_wx_dict[idd]=i.replace(' ','')
      valid_time_idx=valid_time_list[idx]
      wwa_wx_mapping_dict[valid_time_idx]=wwa_wx_dict
   return wwa_wx_mapping_dict


def combine_rtma_grib():
   sector_dirs=glob.glob(LOCAL_DOWNLOAD_FOLDER+'/ndfd/latest/*')
   for sector in sector_dirs:
      try:
         shutil.rmtree(sector+'/rtma') 
      except:
         pass
      try:
         os.makedirs(sector+'/rtma')
      except:
         pass
      rtma_dirs=glob.glob(sector+'/RT*')
      if len(rtma_dirs)>0:
         rtma_wx=glob.glob(rtma_dirs[0]+'/*')
         for wx in rtma_wx:
            wx_file=wx.split('/')[-1]
            wx_all=open(sector+'/rtma/'+wx_file,'wb')
            for rt in rtma_dirs:
               shutil.copyfileobj(open(rt+'/'+wx_file,'rb'),wx_all)
         for RT_dir in rtma_dirs:
            shutil.rmtree(RT_dir)
   return


def convert_to_zarr(folderdate):
   full_ds=None
   client=Client(n_workers=M_WORKERS,processes=True)
   for instance in glob.glob(LOCAL_DOWNLOAD_FOLDER+'/ndfd'+'/*'):
      for region in glob.glob(instance+'/*'):
         ds_master_list=list()
         rs_list=list();ds_list=list()
         region_text=region.split('.')[-1]
         print('begin translating '+region_text+'---------------------------------------')
         zarr_folder=LOCAL_BASE_FOLDER+'/collections/ndfd_xml/'+region_text+'/'+folderdate+'/zarr'
         zarr_folder_temp=LOCAL_DOWNLOAD_FOLDER+'/collections/ndfd_xml/'+region_text+'/'+folderdate+'/zarr'
         zarr_folder_dest=LOCAL_BASE_FOLDER+'/collections/ndfd_xml/'+region_text+'/'+folderdate
         time_file_list=glob.glob(region+'/*')
         for idt,time_range in enumerate(time_file_list):
            for file_name in glob.glob(time_range+'/*'):
               try:
                  wx_param_name=file_name.split('/')[-1].split('.')[1]
               except:
                  wx_param_name='not a grib file'       
               if wx_param_name=='wwa' or wx_param_name=='wx':
                  if ('wwa' in file_name and 'hawaii' in file_name and '001-003' in file_name) or ('wwa' in file_name and 'guam' in file_name and '001-003' in file_name):
                     try:
                        ds=xr.open_dataset(file_name,engine='pynio')
                        if 'SIPD_P0_L1_GME0' in ds.data_vars and 'SIPD_P0_L1_GME1' in ds.data_vars:
                           ds_vt=xr.open_dataset(file_name)
                           valid_time_hw=ds_vt.valid_time.values
                           f1_index=int(len(ds.forecast_time0.values.tolist()))
                           f2_index=int(len(ds.forecast_time1.values.tolist()))
                           ftime_hw_1=valid_time_hw[:f1_index]
                           ds=ds.assign_coords({'ftime_hw_1':ftime_hw_1})
                           lat_0=ds.lat_0.values
                           lon_0=ds.lon_0.values
                           ftime_hw_2=valid_time_hw[f1_index:]
                           ds=ds.assign_coords({'ftime_hw_2':ftime_hw_2})
                           ds=ds.rename({'SIPD_P0_L1_GME0':'wwa1'})
                           ds=ds.rename({'SIPD_P0_L1_GME1':'wwa2'})
                           wwa2=ds.drop(['wwa1','lat_0','lon_0','ftime_hw_1'])
                           wwa2=wwa2.assign_coords({'forecast_time1':wwa2.ftime_hw_2})
                           wwa2=wwa2.drop('ftime_hw_2')
                           wwa2=wwa2.drop('forecast_time0')
                           wwa2=wwa2.rename({'lat_1':'lat_0','lon_1':'lon_0','forecast_time1':'forecast_time0'})
                           wwa2=wwa2.rename({'wwa2':'wwa'})
                           wwa2=wwa2.assign_coords({'forecast_time0':ftime_hw_2,'lat_0':lat_0,'lon_0':lon_0})
                           wwa1=ds['wwa1'].to_dataset()
                           wwa1=wwa1.assign_coords({'forecast_time0':ftime_hw_1})
                           wwa1=wwa1.rename({'wwa1':'wwa'})
                           #time to concat along time dimension
                           ds=xr.concat([wwa1,wwa2],dim='forecast_time0')
                           new_ds=ds
                        if 'SIPD_P0_L1_GME0' in ds.data_vars and 'SIPD_P0_L1_GME1' not in ds.data_vars:
                           ds_vt=xr.open_dataset(file_name)
                           valid_time_hw=ds_vt.valid_time.values
                           ds=ds.rename({'SIPD_P0_L1_GME0':'wwa'})
                           ds=ds.assign_coords({'forecast_time0':valid_time_hw})
                           new_ds=ds
                     except:
                        print('failed')
                  elif ('wwa' in file_name and 'hawaii' in file_name and '004-007' in file_name) or ('wwa' in file_name and 'guam' in file_name and '004-007' in file_name):
                     try:
                        ds_vt=xr.open_dataset(file_name)
                        ds=xr.open_dataset(file_name,engine='pynio')
                        valid_time_hw=ds_vt.valid_time.values
                        ds=ds.assign_coords({'forecast_time0':valid_time_hw})
                        ds=ds.rename({'SIPD_P0_L1_GME0':'wwa'})
                        new_ds=ds
                     except:
                        print('failed')
                  else:
                     new_ds=xr.open_dataset(file_name,engine='pynio')
                  try:
                     shutil.rmtree('wx_wwa_tables')
                  except:
                     pass
                  try:
                     os.makedirs('wx_wwa_tables')
                  except:
                     pass
                  uuid_str=str(uuid.uuid4())
                  os.system("./degrib "+file_name+" -I > ./grib_inventory"+uuid_str+".csv")
                  mapped_ds_list=list()
                  grib_inventory_df=pd.read_csv('./grib_inventory'+uuid_str+'.csv', sep=",")
                  os.remove('./grib_inventory'+uuid_str+'.csv')
                  valid_time_df=grib_inventory_df[grib_inventory_df.columns[6]]
                  del grib_inventory_df
                  valid_time_list=list()
                  for vt in valid_time_df:
                     valid_split=vt[1:].split(' ')
                     valid_day=valid_split[0]
                     valid_time=valid_split[1]
                     year=valid_day.split('/')[2]
                     month=valid_day.split('/')[0]
                     day=valid_day.split('/')[1]
                     time=valid_time+':00'
                     iso_time=year+'-'+month+'-'+day+'T'+time
                     valid_time_list.append(iso_time)
                  new_coord='forecast_time0_'+wx_param_name
                  for dvs in new_ds.data_vars:
                     try:
                        if 'gridrot' in dvs:
                           new_ds=new_ds.drop(dvs)
                        else:
                           new_ds=new_ds.rename({dvs:wx_param_name})
                     except:
                        pass
                  if 'forecast_time0' in new_ds.coords:
                     new_ds=new_ds.rename({'forecast_time0':new_coord})
                     new_ds=new_ds.assign_coords({new_coord: valid_time_list})
                  #we need an exact mapping
                  mapped_ds_list=list()
                  wx_wwa_dict=create_wx_wwa_dict(file_name)
                  for idx,msg in enumerate(new_ds[new_coord].values.astype(str)):
                     mapping_dict=wx_wwa_dict[msg]
                     mapped_vals=np.vectorize(mapping_dict.get)(new_ds[wx_param_name].sel({new_coord:msg}).values)
                     del mapping_dict
                     final_ds=new_ds.sel({new_coord:msg})
                     final_ds[wx_param_name].values=mapped_vals.astype(str)
                     final_ds=final_ds.expand_dims(new_coord)
                     if 'lat_0' in final_ds.dims:
                        final_ds=final_ds.assign_coords({'lat_0':final_ds.lat_0})
                        final_ds=final_ds.assign_coords({'lon_0':final_ds.lon_0})
                     if 'xgrid_0' in final_ds.dims:
                        final_ds=final_ds.assign_coords({'xgrid_0':final_ds.xgrid_0})
                        final_ds=final_ds.assign_coords({'ygrid_0':final_ds.ygrid_0})
                     if not os.path.isdir(zarr_folder_temp):
                        to_zarr=final_ds.astype(str).to_zarr(zarr_folder_temp,mode='w',compute=True,consolidated=True)
                     else:
                        try:
                           param_ds=xr.open_zarr(zarr_folder_temp)
                           if wx_param_name in param_ds.data_vars:
                              append_coord='forecast_time0_'+wx_param_name
                              #try to append to existing coordinate, else merge in with new coord
                              try:
                                 to_zarr=final_ds.astype(str).to_zarr(zarr_folder_temp,mode='a',compute=True,consolidated=True,append_dim=append_coord)
                              except:
                                 to_zarr=final_ds.astype(str).to_zarr(zarr_folder_temp,mode='a',compute=True,consolidated=True)
                           else:
                              to_zarr=final_ds.astype(str).to_zarr(zarr_folder_temp,mode='a',compute=True,consolidated=True)
                        except:
                           print('test')
                     del final_ds
                     print(msg+' '+new_coord+' converted to ugly weather string')
                  del wx_wwa_dict
            #with ugly weather strings dealt with, load all other weather parameters in
            file_to_grab=glob.glob(time_range+'/*.grb')
            files_to_grab=list()
            for f in file_to_grab:
               if 'wwa' not in f and 'wx' not in f:
                  files_to_grab.append(f)
            del file_to_grab
            if len(files_to_grab)>0:
               print('loading into xarray object...')
               if file_name:
                  f_concat_list=list()
                  ds_l=xr.open_dataset(files_to_grab[0],engine='pynio')
                  if 'lat_0' in ds_l.coords:
                     ds_lat_vals=ds_l.lat_0
                  if 'gridlat_0' in ds_l.coords:
                     ds_lat_vals=ds_l.gridlat_0
                  if 'lon_0' in ds_l.coords:
                     ds_lon_vals=ds_l.lon_0
                  if 'gridlon_0' in ds_l.coords:
                     ds_lon_vals=ds_l.gridlon_0
                  for f in files_to_grab:
                     try:
                        try:
                           ds=xr.open_dataset(f,engine='pynio',chunks='auto')
                        except:
                           ds=xr.open_dataset(f,engine='pynio')
                        if 'lat_0' in ds_l.coords:
                           ds=ds.assign_coords({'lat_0':ds_lat_vals})
                        if 'gridlat_0' in ds_l.coords:
                           ds=ds.assign_coords({'gridlat_0':ds_lat_vals})
                        if 'lon_0' in ds_l.coords:
                           ds=ds.assign_coords({'lon_0':ds_lon_vals})
                        if 'gridlon_0' in ds_l.coords:
                           ds=ds.assign_coords({'gridlon_0':ds_lon_vals})
                        ds=set_dims(ds)
                        if ds != None:
                           f_concat_list.append(ds)
                        else:
                           pass
                     except:
                        print(f+' did not match proper dimensionality')
                  ds=xr.merge(f_concat_list)
               for coords in ds.coords:
                  if 'forecast_time' in coords:
                     print('sort by '+coords)
                     ds=ds.sortby(coords)
               ds_master_list.append(ds)
            else:
               print('no files available for time range to append')
         print('combining by coords')
         append_coord_list=list()
         for fcoord in ds.coords:
            if 'forecast_time' in fcoord:
               append_coord_list.append(fcoord)  
         for idd,dataset in enumerate(ds_master_list):
            for wx_var in dataset.data_vars:
               print('setting precision of '+wx_var)
               dtype=precision_mapping[wx_var]['dtype']
               if dtype==float:
                  prec=int(precision_mapping[wx_var]['prec'])
                  dataset[wx_var]=dataset[wx_var].astype(np.float64)
                  da_values=np.around(dataset[wx_var].values.astype(np.float64),decimals=prec)
                  dataset[wx_var].values=da_values
            if idd==0:
               print('initial dataset load...')
               if len(glob.glob(zarr_folder_temp)) > 0:
                  dataset.to_zarr(zarr_folder_temp,mode='a',compute=True,consolidated=True)
               else:
                  dataset.to_zarr(zarr_folder_temp,mode='w',compute=True,consolidated=True)
            else:
               for dv in dataset.data_vars:
                  print(dv+' '+str(idd)+' appending...')
                  dsv=dataset[dv].to_dataset()
                  try:
                     dsv.to_zarr(zarr_folder_temp,mode='a',compute=True,consolidated=True,append_dim='forecast_time0_'+dv)
                  except:
                     dsv.to_zarr(zarr_folder_temp,mode='a',compute=True,consolidated=True)
         del ds_master_list; del ds; del append_coord_list; del dsv
         gc.collect()
         if len(glob.glob(zarr_folder_temp))>0:
            meta(folderdate,region_text)
            subprocess.call(["rsync", "--recursive", zarr_folder_temp, zarr_folder_dest])
         else:
            print(zarr_folder_temp+' did not have contents that met threshold for creation of metadata and rsync to the live directory')
   return


def set_dims(ds):
      wx_param=ds.encoding['source'].split('/')[-1].split('.')[1]
      file_name=ds.encoding['source']
      if 'npacocn' in file_name:
         lon1=ds.lon_0.Lo1[0]
         lon2=ds.lon_0.Lo2[0]
         lon_0_size=ds.dims['lon_0']
         lon_values=np.linspace(lon1,lon2,lon_0_size)
         ds=ds.assign_coords({'lon_0':lon_values})
      if 'oceanic' in file_name:
         ds_lon=ds.lon_0.values
         ds_lon_list=list()
         for dsl in ds_lon:
            if dsl < 128:
               test_dsl=dsl+360
            else:
               test_dsl=dsl
            ds_lon_list.append(test_dsl)
         ds=ds.assign_coords({'lon_0': np.array(ds_lon_list)})
      if 'rtma' in file_name:
         SEARCH_DICT=RTMA_Mapping
         wx_param_name=SEARCH_DICT[wx_param]
         for dcoor in ds.coords:
            if 'initial_time' in dcoor:
               ds=ds.rename({dcoor: 'forecast_time0'})
      else:
         try:
            SEARCH_DICT=NDFD_Mapping
            wx_param_name=SEARCH_DICT[wx_param]
         except:
            SEARCH_DICT=NDFD_EXP_Mapping
            wx_param_name=SEARCH_DICT[wx_param]
      del SEARCH_DICT
      for data_var in ds.data_vars:
         if 'initial_time' in data_var:
            ds=ds.drop(data_var)
         elif 'gridrot' in data_var:
            ds=ds.drop(data_var)
         else:
            ds=ds.rename({data_var:wx_param_name})
      try:
         shutil.rmtree('wx_wwa_tables')
      except:
         pass
      try:
         os.makedirs('wx_wwa_tables')
      except:
         pass
      uuid_str=str(uuid.uuid4())
      os.system("./degrib "+file_name+" -I > ./grib_inventory"+uuid_str+".csv")
      mapped_ds_list=list()
      try:
         grib_inventory_df=pd.read_csv('./grib_inventory'+uuid_str+'.csv', sep=",")
         os.remove('./grib_inventory'+uuid_str+'.csv')
         valid_time_df=grib_inventory_df[grib_inventory_df.columns[6]]
         del grib_inventory_df
         valid_time_list=list()
         for vt in valid_time_df:
            valid_split=vt[1:].split(' ')
            valid_day=valid_split[0]
            valid_time=valid_split[1]
            year=valid_day.split('/')[2]
            month=valid_day.split('/')[0]
            day=valid_day.split('/')[1]
            time=valid_time+':00'
            iso_time=year+'-'+month+'-'+day+'T'+time
            valid_time_list.append(iso_time)
      except:
         import pdb; pdb.set_trace()
      new_coord='forecast_time0_'+wx_param_name
      if 'forecast_time0' in ds.coords:
         ds=ds.rename({'forecast_time0':new_coord})
         try:
            ds=ds.assign_coords({new_coord: valid_time_list})
         except:
            vt_len=len(ds[new_coord].values)
            ds=ds.assign_coords({new_coord: valid_time_list[0:vt_len]})
      else:
         ds=ds.expand_dims(new_coord)
         try:
            ds=ds.assign_coords({new_coord: valid_time_list})
         except:
            pass
      if wx_param_name != 'wx' and wx_param_name != 'wwa':
         dtype=precision_mapping[wx_param_name]['dtype']
         if dtype == int or dtype == float:
            ds[wx_param_name]=ds[wx_param_name].where(ds[wx_param_name] > -90000000000)
            ds[wx_param_name]=ds[wx_param_name].fillna(int(-9999))
         else:
            ds[wx_param_name]=ds[wx_param_name].where(ds[wx_param_name] > -90000000000)
         ds[wx_param_name]=ds[wx_param_name].astype(dtype)
      if 'lat_0' in ds.dims:
         ds[wx_param_name]=ds[wx_param_name].chunk({'lat_0':128,'lon_0':128})
      if 'xgrid_0' in ds.dims:
         ds[wx_param_name]=ds[wx_param_name].chunk({'xgrid_0':128,'ygrid_0':128})
      return ds


def meta(folderdate,region_text):
   zarr_folder=LOCAL_DOWNLOAD_FOLDER+'/collections/ndfd_xml/'+region_text+'/'+folderdate+'/zarr'
   ds=xr.open_zarr(zarr_folder)
   ds_dict=ds.to_dict(data=False)
   col_dict={}
   col_dict['collection_name']='ndfd_xml'
   dim_list=list(ds_dict['dims'].keys())
   dim_list.append('forecast_time')
   col_dict['dimensions']=dim_list
   col_dict['parameters']=list(ds_dict['data_vars'].keys())
   long_name=list()
   for dv in ds.data_vars:
      long_name.append(dv)
   col_dict['long_name']=long_name
   forecast_time_list=list()
   for dim in ds.dims:
      if 'forecast_time' in dim:
         ftime=ds[dim].values.tolist()
         for t in ftime:
            if isinstance(t,str):
               forecast_time_list.append(t)
   forecast_time_list = list(dict.fromkeys(forecast_time_list))
   col_dict['forecast_time']=sorted(forecast_time_list)
   for d in col_dict['dimensions']:
      if 'forecast' not in d:
         col_dict[d]=ds[d].values.tolist()
   cycle=folderdate
   try:
      os.makedirs(LOCAL_BASE_FOLDER+'/collections/ndfd_xml/'+region_text+'/'+cycle)
   except:
      pass
   with open(LOCAL_BASE_FOLDER+'/collections/ndfd_xml/'+region_text+'/'+cycle+'/'+cycle+'_ndfd_collection.json','w') as json_final:
      json_list=list()
      json_list=[col_dict]
      json.dump(json_list,json_final,indent=2, sort_keys=True)
   return


if __name__ == '__main__':
    folderdate='latest'
    try:
       shutil.rmtree(LOCAL_DOWNLOAD_FOLDER)
       print('deleting previous download directory')
    except:
       pass
    try:
       os.makedirs(LOCAL_BASE_FOLDER+'/collections/ndfd_xml/')
    except:
       pass
    for BASE_URL in BASE_URL_LIST:
       regions=getLinks(BASE_URL)
       for region in regions:
          if region[:-1] in LOCATIONS:
              periods=getLinks(BASE_URL + region)
              for period in periods:
                  params=getLinks(BASE_URL + region + period)
                  for param in params:
                      downloadfile(region, period, param, folderdate)
    combine_rtma_grib()
    convert_to_zarr(folderdate)
    try:
       shutil.rmtree(LOCAL_DOWNLOAD_FOLDER)
    except:
       print('directory not available to remove')
