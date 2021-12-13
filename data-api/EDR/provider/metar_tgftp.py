import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
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


VERSION = '0.0.1'
headers_ = {
    'X-Powered-By': 'Environmental Data Retrieval API {}'.format(VERSION)
}

class MetarTgftpProvider(BaseProvider):

    def __init__(self, dataset, config):
#        """initializer"""
        self.name = 'metar_tgftp'
#        self.source_dir = os.path.join(os.path.dirname(__file__), 
#                                       config['datasets'][dataset]['provider']['data_source'])
        self.config = config
        self.source_url = config['datasets'][dataset]['provider']['data_source']
#        self.db = config['datasets']['metar']['provider']['station_list_db']

        self.MDL_Bulletin = bulletin.Bulletin()
        self.MDL_Annex3D = metarDecoders.Annex3()
        self.MDL_FMH1D = metarDecoders.FMH1()
        self.MDL_Annex3E = metarEncoders.Annex3()
        self.MDL_FMH1E = metarEncoders.FMH1()
        self.db = config['datasets']['metar']['provider']['station_list_db']

    def getSiteInfo(self, station_id):
        conn = sqlite3.connect(self.db)
        try:
            conn.enable_load_extension(True)
        except AttributeError as err:
            print('Extension loading not enabled: {}'.format(err))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT load_extension('mod_spatialite.so')")
        except sqlite3.OperationalError as err:
            try:
                cursor.execute("SELECT load_extension('mod_spatialite')")
            except sqlite3.OperationalError as err:
                try:
                    cursor.execute("SELECT load_extension('libspatialite')")
                except sqlite3.OperationalError as err:                
                    print('Extension loading error: {}'.format(err))
        cursor.fetchall()
        sql_query='select * from stations where icao=="'+station_id+'";';
        stn_data = cursor.execute(sql_query)
        for row_data in stn_data:
            row_data = dict(row_data)  # sqlite3.Row is doesnt support pop
        conn.close()
        return row_data


    def determine_decoder(self,site_name):
       if site_name[0] == 'K' or site_name[:2] in ['PA','PH','PG','TJ']:
          decoder = self.MDL_FMH1D
          xmlEncoder = self.MDL_FMH1E
       else:
          decoder = self.MDL_Annex3D
          xmlEncoder = self.MDL_Annex3E
       return decoder, xmlEncoder


    def query(self, dataset, qtype, coords, time_range, z_value, params, identifier, outputFormat):
            #request to tgftp will go here with response being text first, then we can translate to other output formats
            identifier_trim=identifier.split('_')[0]
            if 'collective' in request.url:
               cycle=request.url.split('?')[0].split('/')[-1].split('_')[1]
               rq_url=self.source_url+'/cycles/'+cycle+'.TXT'
               data=requests.get(rq_url)
               tac=data.text
               tac_text=tac
            if 'raw' in identifier:
               if 'latest' in request.url:
                  rq_url=self.source_url+'/stations/'+identifier_trim+'.TXT'
                  data=requests.get(rq_url)
                  tac=data.text.split('\n')[1]
                  site_name=tac.split(' ')[0]
                  metar='METAR '+tac
                  tac_text=data.text
                  decoder,xmlEncoder=self.determine_decoder(site_name)
                  decodedMetar=decoder(metar)
                  document=xmlEncoder(decodedMetar,metar)
                  self.MDL_Bulletin.cacheDocument(document,'AAA KKCI YYgg')
                  iwxxm=self.MDL_Bulletin.makeBulletin()
               else:
                  site_name=request.url.split('?')[0].split('/')[-1].split('_')[0]
                  cycle=request.url.split('?')[0].split('/')[-1].split('_')[1]
                  rq_url=self.source_url+'/cycles/'+cycle+'.TXT'
                  data_rq = urlopen(rq_url)  
                  data_read=data_rq.read()
                  df=pd.read_csv(io.StringIO(data_read.decode("ISO-8859-1")),sep='\n')
                  df.columns=['metars']
                  tac=df[df['metars'].str.contains(site_name)].iloc[0].to_list()[0]
                  tac_text=tac
                  metar='METAR '+tac
                  decoder,xmlEncoder=self.determine_decoder(site_name)
                  decodedMetar=decoder(metar)
                  document=xmlEncoder(decodedMetar,metar)
                  self.MDL_Bulletin.cacheDocument(document,'AAA KKCI YYgg')
                  iwxxm=self.MDL_Bulletin.makeBulletin()
            elif 'decoded' in identifier:
               if 'latest' in request.url:
                  rq_url=self.source_url+'/decoded/'+identifier_trim+'.TXT'
                  data=requests.get(rq_url)
                  tac=data.text.split('\n')[1]
                  tac_text=data.text
                  site_name=tac.split(' ')[0]
               else: 
                  tac_text='Decoded METAR not yet available for times other than "latest"'
            else:
               identifier=request.url.split('/')[-1].split('?')[0].replace('_','.')
               rq_url=self.config['datasets'][dataset]['provider']['t1t2_data_source']+identifier+'.txt'
               data=requests.get(rq_url)
               if data.status_code==404:
                  #inconsistent .txt vs. ..txt
                  rq_url=rq_url.replace('.txt','..txt')
                  data=requests.get(rq_url)
                  tac_text=data.text
               if data.status_code==200:
                  tac_text=data.text
               tac_list=tac_text.split('\n')
               metar_list=list();decoded_list=list()
               for ta in tac_list:
                  if 'METAR' in ta or 'SPECI' in ta:
                     site_name=ta.split(' ')[1]
                     decoder,xmlEncoder=self.determine_decoder(site_name) 
                     decodedMetar=decoder(ta)
                     metar=ta
                     if outputFormat=='xml':
                        document=xmlEncoder(decodedMetar,metar)
                        self.MDL_Bulletin.cacheDocument(document,'AAA KKCI YYgg')
                        iwxxm_loc=self.MDL_Bulletin.makeBulletin()
                        metar_list.append(iwxxm_loc)
                     if outputFormat=='json':
                        decoded_list.append(decodedMetar)
               if outputFormat=='json':
                  decodedMetar=decoded_list
               #ask mark o what to do with a list of iwxxm:       
               iwxxm=''.join(metar_list)
            if outputFormat == 'ascii':
                headers_['Content-type'] = 'text/ascii'
                return tac_text, 'no_delete'
            if outputFormat == 'xml':
                headers_['Content-type'] = 'application/xml'
                return iwxxm, 'no_delete'
            if outputFormat == 'json':
                headers_['Content-type'] = 'application/json'
                return json.dumps(decodedMetar), 'no_delete'
                
