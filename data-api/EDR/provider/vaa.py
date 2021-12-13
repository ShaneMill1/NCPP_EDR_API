import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json

from EDR.provider.base import BaseProvider
from EDR.provider import vaaDecoder
from EDR.provider import vaaEncoder

import os
import random
import time

VERSION = '0.0.1'
headers_ = {
    'X-Powered-By': 'Environmental Data Retrieval API {}'.format(VERSION)
}

class VaaProvider(BaseProvider):

    def __init__(self, dataset, config):
        """initializer"""
        self.config = config
        self.source_dir = os.path.join(os.path.dirname(__file__), 
                                       config['datasets'][dataset]['provider']['data_source'])
        self.decoder = vaaDecoder.Decoder()
        self.encoder = vaaEncoder.Encoder()
        self.tacs = []
        for f in [os.path.join(self.source_dir, x) 
                  for x in os.listdir(self.source_dir) if x[:2] == 'FV']:
            _fh = open(f, 'r')
            self.tacs.append(_fh.read())
            _fh.close()
        random.seed()

    def query(self, dataset, qtype, coords, time_range, z_value, params, identifier, outputFormat):
        if qtype == 'point':
            try:
               for idx,t in enumerate(self.tacs):
                  t_all=self.tacs[idx].split('\n')
                  t_el=t_all[2].split(' ')
                  dd=t_all[4].split(' ')[1].split('/')[0][-2:]
                  tac_string=t_el[0]+t_el[1]+'.'+t_el[2]
                  id_search=identifier.split('_')
                  id_manipulate=id_search[1][0:4]
                  id_match=id_search[0]+'.'+dd+id_manipulate
                  print(id_match + '==' + tac_string)
                  if str(tac_string)[:-6] in str(id_match)[:-6]:
                     print(str(tac_string)[:-2])
                     print(str(id_match)[:-2])
                     tac = self.tacs[idx]
                  else:
                     print('no match')
            except IndexError:
                headers_['Content-type'] = 'text/ascii'
                return 'No product found'
                
            if outputFormat == 'tac':
                headers_['Content-type'] = 'text/ascii'
                return tac, 'no_delete'

            decodedTAC = self.decoder(tac)
            if outputFormat == 'json':
                headers_['Content-type'] = 'application/json'
                return json.dumps(decodedTAC), 'no_delete'

            else:
                headers_['Content-type'] = 'application/xml'
                decodedTAC['translatedBulletinReceptionTime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
                decodedTAC['translatedBulletinID'] = 'FVXX23KNES%s' % time.strftime('%d%H%M')
                return self.encoder(decodedTAC, tac). 'no_delete
                
        elif qtype == 'multipoint':
            return json.dumps('Multi-point queries not supported yet.')
