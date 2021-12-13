from bs4 import BeautifulSoup
import cycle_info
from datetime import datetime
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
   date=datetime.today().strftime('%Y%m%d')
   if model=='nbm':
      url_dir='https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/blend.'+str(date)+'/'+str(cycle)+'/grib2/'
   response = requests.get(url_dir)
   print(response)
   dir_s = BeautifulSoup(response.text, 'html.parser')
   dir_list=list()
   data_dir = cycle_dir+'/'
   if model=='nbm':
      for e in dir_s.find_all('a')[1:]:
         grib_link=url_dir+e.text
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
   model_ingest('00z','nbm','./data/')
