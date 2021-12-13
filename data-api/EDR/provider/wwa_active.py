import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
from EDR.provider import bulletin
from EDR.provider.base import BaseProvider
import requests
import os
import random
import time
from flask import request
import pandas as pd
from urllib.request import urlopen
import io
from bs4 import BeautifulSoup
import xmltodict

VERSION = '0.0.1'
headers_ = {
    'X-Powered-By': 'Environmental Data Retrieval API {}'.format(VERSION)
}

class WWAActiveProvider(BaseProvider):

    def __init__(self, dataset, config):
#        """initializer"""
        self.config = config
        self.source_url = config['datasets']['wwa_active']['provider']['data_source']

    def query(self, dataset, qtype, coords, time_range, z_value, params, identifier, outputFormat):
       source_url=self.source_url
       wwa_home=requests.get(source_url)
       soup=BeautifulSoup(wwa_home.text,'html.parser')
       wwa_home.close()
       entries=soup.find_all('entry')
       header=soup.prettify().split('<entry>')[0]
       header=header.replace("<?xml-stylesheet href=\'capatom.xsl\' type=\'text/xsl\'?>\n",'')
       for e in entries:
          cap_event=e.find_all('cap:event')[0].string.replace(' ','_').lower()
          cap_effective=e.find_all('cap:effective')[0].string
          try:
             loc_id=e.find_all('value')[-1].string.split('.')[2]
             item_name=cap_event+'_'+loc_id+'_'+cap_effective
          except:
             item_name=cap_event+'_'+cap_effective
          if item_name==identifier:
             output=e 
             break
       output=header+output.prettify()+'</feed>'
       if outputFormat == 'cap':
          headers_['Content-type'] = 'text/xml'
          return output, 'no_delete'
       if outputFormat == 'json':
          headers_['Content-type'] = 'application/json'
          output=xmltodict.parse(output)
          return json.dumps(output), 'no_delete'


         
