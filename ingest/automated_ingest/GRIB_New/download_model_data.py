#!/opt/conda/envs/env/bin/python
import argparse
import os
from multiprocessing import Pool
from datetime import date
from datetime import datetime,timedelta
import shutil
import glob

def sys_request(i):
   today=date.today()
   today=today.strftime("%Y%m%d")
   d=today+model_run
   rq='perl get.pl data '+d+' ' +str(i)+' '+str(i)+' '+str(i)+' all all '+ file_location+'/data_download'+' '+ model
   os.system(rq)
   return



def multi_process_loop(model,model_run,file_location):
   hour_list=list()
   os.makedirs(file_location+'/data_download')
   for i in range(0, 387,3):
      sys_request(i)
   return


def combine_grib(model,model_run,file_location):
   files=glob.glob(file_location+'/data_download/*[!.idx]')
   os.makedirs(file_location+'/combined_data')
   with open(file_location+'/combined_data/combined.grb', 'wb') as outfile:
      for filename in files:
          with open(filename, 'rb') as readfile:
              shutil.copyfileobj(readfile, outfile)
   shutil.rmtree(file_location+'/data_download')
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
