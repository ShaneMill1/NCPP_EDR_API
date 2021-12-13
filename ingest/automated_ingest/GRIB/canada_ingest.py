from bs4 import BeautifulSoup
import cycle_info
import datetime
import multiprocessing
import os
import requests
import shutil
from vdvt import *
import wget
import resource
import numpy as np

def model_download(model,url,data_dir):
   file_download = wget.download(url,data_dir)
   os.rename(file_download,file_download.replace('.grib2','.grb'))
   return


def model_ingest(cycle,model,ingest_path):
   model=model.lower()
   ingest_path=ingest_path+model
   cycle_dir=ingest_path+"/"+cycle
   grib_list=list()
   if os.path.exists(cycle_dir) and os.path.isdir(cycle_dir):
     shutil.rmtree(cycle_dir)
   os.makedirs(cycle_dir)
   model_short=model.split('_')[0]
   cycle=cycle.strip('z')
   cycle=cycle.split(':')[-1]
   if model=='gem_25km':
      url_dir='https://dd.weather.gc.ca/model_gem_global/25km/grib2/lat_lon/'+cycle+'/'
   print(url_dir)
   response = requests.get(url_dir)
   print(response)
   dir_s = BeautifulSoup(response.text, 'html.parser')
   dir_list=list()
   data_dir = cycle_dir+'/'
   if model=='gem_25km':
      for e in dir_s.find_all('a')[5:]:
         forecast_hour=e.get('href')
         url=url_dir+forecast_hour
         grib_response=requests.get(url)
         dir_grib=BeautifulSoup(grib_response.text, 'html.parser')
         for f in dir_grib.find_all('a'):
            grib_link=url+f.get('href')
            if str(np.datetime64('today')).replace('-','') in grib_link:
               grib_list.append(grib_link)
   print('begin downloading '+model+' files for '+cycle)
   cpus = multiprocessing.cpu_count()
   max_pool_size = 7
   pool = multiprocessing.Pool(cpus if cpus < max_pool_size else max_pool_size)
   for url in grib_list:
      pool.apply_async(model_download, args=(model,url,data_dir))
   pool.close()
   pool.join()

   return 'finished downloading '+ cycle

if __name__ == "__main__":
   model_ingest('00z','gem_global','./data/')
