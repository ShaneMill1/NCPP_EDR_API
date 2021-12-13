import sqlite3
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import copy
from EDR.provider.base import BaseProvider
from EDR.provider import tafDecoder
from EDR.provider import tafEncoder
import re
VERSION = '0.0.1'
headers_ = {
    'X-Powered-By': 'Environmental Data Retrieval API {}'.format(VERSION)
}

class TafProvider(BaseProvider):

    def __init__(self, dataset, config):
        """initializer"""
        self.config = config
        self.source_url = config['datasets'][dataset]['provider']['data_source']
        self.db = config['datasets']['taf']['provider']['station_list_db']
        self.decoder = tafDecoder.Decoder()
        self.IWXXMEncoder = tafEncoder.Encoder()

    def query(self, dataset, qtype, coords, time_range, z_value, params, identifier, outputFormat):
        if qtype == 'point':
            try:
                rawText, stationInfo = self.getNearestTaf(coords[1], coords[0])
                decodedTAF = self.decoder(rawText)
                decodedTAF['ident']['location'] = "{0} {1}".format(stationInfo['lat'],
                                                                   stationInfo['lon'])
                decodedTAF['ident']['name'] = stationInfo['station']
                #
                # AWC strips off the header
                decodedTAF['bbb'] = ' '

                if outputFormat == 'json':
                    headers_['Content-type'] = 'application/json'
                    return json.dumps(decodedTAF), 'no_delete'

                elif outputFormat == 'xml':
                    headers_['Content-type'] = 'application/xml'
                    return self.IWXXMEncoder(decodedTAF, rawText), 'no_delete'

                elif outputFormat == 'tac':
                    headers_['Content-type'] = 'text/ascii'
                    return rawText,'no_delete'

            except KeyError:
                return json.dumps('No airport was found nearby in AWC database at that location.')
                
        elif qtype == 'multipoint':
            return json.dumps('Multi-point queries not supported yet.')

    def requests_retry_session(self, retries=5, backoff_factor=0.3, status_forcelist=(500, 502, 504, 404),
                               session=None, ):
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

    def strip_html(self, html_page):
        #
        # At this time, based on AWC web page behavior, only
        # one--the most recently issued TAF for the airport--
        # is returned.
        #
        raw = html_page.split("<code>")[1]
        #
        # Remove remaining html cruft to get TAC form.
        return re.sub(r'/>|[a-z<>&;]','',raw[:raw.find("</code>")])

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
        sql_query = "SELECT station, icao, X(geometry) AS lon, Y(geometry) AS lat, Min(ST_Distance(MakePoint({0}, {1}), GEOMETRY, 1))/1000.0 "\
            "AS dist_km FROM stations WHERE aerodrome = 'T';".format(str(lon), str(lat))
        stn_data = cursor.execute(sql_query)
        for row_data in stn_data:
            row_data = dict(row_data)  # sqlite3.Row is doesnt support pop
            break

        conn.close()
        return row_data

    def getNearestTaf(self, lat, lon):

        siteInfo = self.getSiteInfo(lat, lon)
        restString = "{0}?ids={1}&format=raw&metars=off&layout=off".format(self.source_url, siteInfo['icao'])
        r = self.requests_retry_session().get(restString, stream=True)

        rawText = self.strip_html(r.text)
        return rawText, siteInfo
