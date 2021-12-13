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
import datetime

def model_download(model,url,data_dir):
   file_download = wget.download(url,data_dir)
   validityDate, validityTime = get_vdvt(file_download)
   validityTime = format(validityTime,"04")
   filename = data_dir+model+'.'+str(validityDate) + 'T' + str(validityTime) + '00Z.grb'
   os.rename(file_download,filename)
   return

def model_ingest(cycle,model,ingest_path):
   model=model.lower()
   ingest_path=ingest_path+model
   cycle_dir=ingest_path+"/"+cycle
   if os.path.exists(cycle_dir) and os.path.isdir(cycle_dir):
     shutil.rmtree(cycle_dir)
   os.makedirs(cycle_dir)
   model_short=model.split('_')[0]
   time_z, url_dir=cycle_info.info(model_short,cycle)
   url_dir=url_dir+'/atmos/'
   print(url_dir+'/')
   response = requests.get(url_dir)
   print(response)
   dir_s = BeautifulSoup(response.text, 'html.parser')
   dir_list=list()
   data_dir = cycle_dir+'/'
   if model=='gfs_100':
      for e in dir_s.find_all('a'):
         if '.pgrb2.1p00' in e.text and '.idx' not in e.text and '.anl' not in e.text:
            fn=url_dir+e.text
            dir_list.append(fn)
   if model=='gfs_050':
      for e in dir_s.find_all('a'):
         if '.pgrb2.0p50' in e.text and '.idx' not in e.text and '.anl' not in e.text:
            fn=url_dir+e.text
            dir_list.append(fn)
   if model=='gfs_025':
      for e in dir_s.find_all('a'):
         if '.pgrb2.0p25' in e.text and '.idx' not in e.text and '.anl' not in e.text:
            fn=url_dir+e.text
            dir_list.append(fn)       
   if model=='nam_32km':
      for e in dir_s.find_all('a'):
         if '.t'+cycle in e.text and 'awip32' in e.text and '.idx' not in e.text and '.anl' not in e.text and 'bufr' not in e.text:
            fn=url_dir+e.text
            dir_list.append(fn)
   if model=='hrrr':
      for e in dir_s.find_all('a'):
         if '.t'+cycle in e.text and 'wrfsfc' in e.text and '.idx' not in e.text and '.anl' not in e.text and 'bufr' not in e.text:
            fn=url_dir+e.text
            dir_list.append(fn)
   if model=='rap':
      for e in dir_s.find_all('a'):
         if '.t'+cycle in e.text and 'awp252' in e.text and '.idx' not in e.text and '.anl' not in e.text and 'bufr' not in e.text:
            fn=url_dir+e.text
            dir_list.append(fn)

   print('begin downloading '+model+' files for '+cycle)
   cpus = multiprocessing.cpu_count()
   max_pool_size = 6
   pool = multiprocessing.Pool(cpus if cpus < max_pool_size else max_pool_size)
   for url in dir_list:
      pool.apply_async(model_download, args=(model,url,data_dir))
   pool.close()
   pool.join()

   return 'finished downloading '+ cycle

if __name__ == "__main__":
#   cycle_array=['00z','06z','12z','18z']
#   for c in cycle_array:
#      print(c)
#      model_ingest(c,'hrrr')
   model_ingest('00','gfs_100','./data/')

