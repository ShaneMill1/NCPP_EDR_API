import argparse
import datetime
import glob
import os
import shutil

def cleanup(data_dir,model):
   date_now=datetime.datetime.now()
   days_ago=datetime.timedelta(days=7)
   comp_date= date_now-days_ago
   for zarr_store in glob.glob(data_dir+'/collections/'+model+'/*'):
      path=os.path.dirname(zarr_store)
      date_string=os.path.basename(zarr_store)
      if 'T' in date_string:
         date_string=datetime.datetime.fromisoformat(date_string)
         if date_string < comp_date:
            to_delete=path+'/'+date_string.isoformat()
            print('Removing '+to_delete)
            shutil.rmtree(to_delete)
      else:
         pass
   #double check that the data directories are gone too
   for data_dir in glob.glob(data_dir+'/'+model+'/*'):
      print('Removing data directory '+data_dir)
      shutil.rmtree(data_dir)
   return

if __name__=="__main__":
   #args to be taken in eventually as user args. hardcoding for right now
   parser = argparse.ArgumentParser(description='Model data cleanup')
   parser.add_argument('model', type = str, help = 'Enter the model (ex: gfs_100)')
   parser.add_argument('data_dir', type = str, help = 'Enter the data directory (ex: /data/collections/)')
   args=parser.parse_args()
   model=args.model
   data_dir=args.data_dir
   cleanup(data_dir,model)
