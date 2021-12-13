import sqlite3
from metar import Metar
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import copy
from datetime import datetime
import EDR.isodatetime.data as timedata
import EDR.isodatetime.parsers as tparser
from EDR.templates import edrpoint
from EDR.formatters import metar_to_covjson
import json
from EDR.provider.base import BaseProvider, ProviderConnectionError, ProviderQueryError
from EDR.provider import InvalidProviderError

from EDR.provider import metarDecoders
from EDR.provider import metarEncoders
from EDR.provider import bulletin


class MetarProvider(BaseProvider):

    def __init__(self, dataset, config):
        """initializer"""
        self.config = config
        self.source_url = config['datasets'][dataset]['provider']['data_source']
        self.db = config['datasets']['metar']['provider']['station_list_db']

        self.MDL_Annex3D = metarDecoders.Annex3()
        self.MDL_FMH1D = metarDecoders.FMH1()
        self.MDL_Annex3E = metarEncoders.Annex3()
        self.MDL_FMH1E = metarEncoders.FMH1()
        self.MDL_Bulletin = bulletin.Bulletin()

    def query(self, dataset, qtype, coords, time_range, z_value, params, identifier, outputFormat):

        if qtype == 'point':
            if outputFormat == 'CoverageJSON':
                
                metars, slat, slon = self.getNearestObs(coords[1], coords[0], time_range, outputFormat)
                metars.sort(key=lambda x:x.time)
                output = metar_to_covjson.get_point(metars, slat, slon, params, self.config)
                return json.dumps(output), 'no_delete'

            elif outputFormat == 'xml':
                iwxxmDocuments, slat, slon = self.getNearestObs(coords[1], coords[0], time_range, outputFormat)
                for document in iwxxmDocuments:
                    self.MDL_Bulletin.cacheDocument(document,'AAA KKCI YYgg')

                return self.MDL_Bulletin.makeBulletin(),'no_delete'
            


        elif qtype == 'multipoint':

            output = {}
            output['type'] = "CoverageCollection"
            output['domainType'] = "PointSeries"
            output['parameters'] = None
            output['coverages'] = [] 
            for coord in coords:
                result = {}
                metars, slat, slon = self.getNearestObs(coord[1], coord[0], time_range, outputFormat)
                metars.sort(key= lambda x:x.time)
                result = metar_to_covjson.get_point(metars, slat, slon, params, self.config)
                             
                if output['parameters'] is None:
                    output['parameters'] = copy.deepcopy(result['parameters'])
                    output['referencing'] = copy.deepcopy(result['domain']['referencing'])
                
                coverage = {}
                coverage['type'] = copy.deepcopy(result['type'])
                coverage['domain'] = {}
                coverage['domain']['type'] = copy.deepcopy(result['domain']['type'])
                coverage['domain']['axes'] = copy.deepcopy(result['domain']['axes'])
                coverage['ranges'] = copy.deepcopy(result['ranges'])
                output['coverages'].append(coverage)        
        
            return json.dumps(output),'no_delete'


    def requests_retry_session(self, 
        retries=5,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504, 404),
        session=None,):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session


    def strip_html(self, html_page, xml):
        parts = html_page.split("<code>")
        metars = []
        for p in parts:
            end_of_str = p.find("</code>")
            if end_of_str > -1:
                metar = p[:end_of_str]
                if xml:
                    metars.append('METAR %s' % metar)
                else:
                    try:
                        metars.append(Metar.Metar(metar))
                    except:
                        print("Parser error: " + metar)
                    
        return metars

    def getSiteInfo(self, lat, lon):

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
        sql_query = "SELECT station, icao, X(geometry) as lon,Y(geometry) as lat, Min(ST_Distance(MakePoint("+str(lon)+", "+str(lat)+"), GEOMETRY, 1)) / 1000.0 AS dist_km FROM stations;"
        stn_data = cursor.execute(sql_query)
        for row_data in stn_data:
            row_data = dict(row_data)  # sqlite3.Row is doesnt support pop
        conn.close()
        return row_data


    def getNearestObs(self, lat, lon, timeinterval, outputFormat):

        site = self.getSiteInfo(lat, lon)
        if outputFormat == 'xml':
            if site['icao'][0] == 'K' or site['icao'][:2] in ['PA','PH','PG','TJ']:
                decoder = self.MDL_FMH1D
                xmlEncoder = self.MDL_FMH1E
            else:
                decoder = self.MDL_Annex3D
                xmlEncoder = self.MDL_Annex3E
        r = self.requests_retry_session().get(
            self.source_url + '?ids='+site['icao']+'&format=raw&hours=36&taf=off&layout=off', stream=True)
        
        all_metars = self.strip_html(r.text, outputFormat == 'xml')
        valid_time_metars = []

        for m in all_metars:
            if outputFormat == 'xml':
                decodedMetar = decoder(m)
                decodedMetar['ident']['position'] = '%.3f %.3f' % (float(site['lat']),float(site['lon']))
                decodedMetar['ident']['name'] = site['station']
                valid_time_metars.append(xmlEncoder(decodedMetar,m))

            else:
                if (timeinterval is None) or timeinterval.get_is_valid(tparser.TimePointParser().parse(m.time.isoformat()+'Z')):
                    valid_time_metars.append(m)

        return valid_time_metars, site['lat'], site['lon'] 
