import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.auth import HTTPBasicAuth
import json
from EDR.provider import bulletin
from EDR.provider.base import BaseProvider
from EDR.provider import metarDecoders
from EDR.provider import metarEncoders
import requests
import os
import random
import time
import sqlite3
from flask import request
import pandas as pd
from urllib.request import urlopen
import io
from datetime import datetime, timedelta
VERSION = '0.0.1'
headers_ = {
    'X-Powered-By': 'Environmental Data Retrieval API {}'.format(VERSION)
}
class WIFSPNGProvider(BaseProvider):
    def __init__(self, dataset, config):
#        """initializer"""
        self.name = 'wifs_png'
        self.config = config
        self.source_url = config['datasets'][dataset]['provider']['data_source']
        
    def query(self, dataset, qtype, coords, time_range, z_value, params, identifier, outputFormat):
       uname='WAFSKDCA02';pword='%Wcw$859'
       today=datetime.today().strftime('%Y%m%d')
       yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
       items_map={'SWH-H_North_Atlantic_US': 'PGAE05_KKCI',
                  'SWH-I_North_Pacific_US': 'PGBE05_KKCI',
                  'SWH-M_North_Pacific_US': 'PGDE29_KKCI',
                  'SWH-A_Americas_US': 'PGEE05_KKCI',
                  'SWH-F_North_Pacific_US': 'PGGE05_KKCI',
                  'SWH-B1_Americas_AFI_US': 'PGIE05_KKCI'}
       for key, value in items_map.items():
          if key in identifier:
             data_url=self.source_url+'/'+str(today)+'_'+identifier.split('_')[-1]+'_'+items_map[key]+'.png'
             response = requests.get(data_url, auth=HTTPBasicAuth(uname,pword))
             if response.status_code==200:
                png=response.content
             else:
                response.close()
                data_url=self.source_url+'/'+str(yesterday)+'_'+identifier.split('_')[-1]+'_'+items_map[key]+'.png'
                response = requests.get(data_url, auth=HTTPBasicAuth(uname,pword))
                png=response.content 
             response.close()
       if outputFormat == 'png':
          headers_['Content-type'] = 'image/png'
          return png, 'no_delete'
