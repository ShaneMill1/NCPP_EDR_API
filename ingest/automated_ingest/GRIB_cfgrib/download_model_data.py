#!/opt/conda/envs/env/bin/python
import argparse
import os
from multiprocessing import Pool
from datetime import date
from datetime import datetime,timedelta
import shutil
import glob
import requests
from bs4 import BeautifulSoup
import urllib



def sys_request(i):
   try:
      file_name=i.split('/')[-1]
      urllib.request.urlretrieve(i,file_location+'/data_download/'+file_name)
      print(file_name + ' downloaded')
   except:
      pass
   return


def multi_process_loop(model,model_run,file_location):
   hour_list=list()
   os.makedirs(file_location+'/data_download')
   this_day=date.today().isoformat().replace('-','')
   url='https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.'+this_day+'/'+model_run+'/atmos/'
   file_list=[]
   rq=requests.get(url)
   html=rq.text
   soup=BeautifulSoup(html,'html.parser')
   for hr in soup.find_all('a'):
      file_text=hr.text
      if 'gfs.t00z.pgrb2b.1p00' in hr.text and '.idx' not in hr.text and '.anl' not in hr.text:
         url_get=url+hr.text
         file_list.append(url_get)
   pool=Pool()
   pool.map(sys_request,file_list)
   return


def combine_grib(model,model_run,file_location):
   print('combining grib files')
   files=glob.glob(file_location+'/data_download/*[!.idx]')
   os.makedirs(file_location+'/combined_data')
   with open(file_location+'/combined_data/combined.grb', 'wb') as outfile:
      for filename in files:
          with open(filename, 'rb') as readfile:
              shutil.copyfileobj(readfile, outfile)
   print('removing downloaded grib files')
   shutil.rmtree(file_location+'/data_download')
   print('concat complete, ready to convert grib to zarr')
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
   file_location=root_dir+'/'+model+'/'+today+'/'
   try:
      shutil.rmtree(file_location)
   except:
      pass
   os.makedirs(file_location)
   file_location=root_dir+'/'+model+'/'+today+'/'
   multi_process_loop(model,model_run,file_location)
   combine_grib(model,model_run,file_location)
