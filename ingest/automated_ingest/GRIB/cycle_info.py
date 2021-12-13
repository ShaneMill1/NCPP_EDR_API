import requests
from bs4 import BeautifulSoup
import datetime

def info(model,cycle):
   model=model.lower()
   url='https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_1p00.pl'
   r = requests.get(url)
   soup = BeautifulSoup(r.text, 'html.parser')
   cycle=cycle.split('T')[1][0:2]
   #formulate 00z and date relation
   date_url1=soup.find_all('a')[0].text
   date_url2=soup.find_all('a')[1].text
   if model=='gfs':
      try:
         url_dir = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/'+model+'/prod/'+date_url1+'/'+cycle+'/'
         time_z=date_url1.strip('.')[1]
      except:
         url_dir = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/'+model+'/prod/'+date_url2+'/'+cycle+'/'
         time_z=date_url2.strip('.')[1]
      print(url_dir)
   if model=='nam':
      try:
         url_dir = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/'+model+'/prod/'+date_url1+'/'
         time_z=date_url1.strip('.')[1]
      except:
         url_dir = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/'+model+'/prod/'+date_url2+'/'
         time_z=date_url2.strip('.')[1]
      print(url_dir)
   if model=='hrrr':
      try:
         url_dir = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/'+model+'/prod/'+date_url1+'/conus/'
         time_z=date_url1.strip('.')[1]
      except:
         url_dir = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/'+model+'/prod/'+date_url2+'/conus/'
         time_z=date_url2.strip('.')[1]
      print(url_dir)
   if model=='rap':
      try:
         url_dir = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/'+model+'/prod/'+date_url1+'/'
         time_z=date_url1.strip('.')[1]
      except:
         url_dir = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/'+model+'/prod/'+date_url2+'/'
         time_z=date_url2.strip('.')[1]
      print(url_dir)
   return time_z, url_dir

if __name__ == "__main__":
   info('gfs','00z')



