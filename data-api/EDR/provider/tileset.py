from EDR.provider.hgt import HGT
import math
import requests
from requests.adapters import HTTPAdapter
import gzip
import numpy as np

base_url = "https://elevation-tiles-prod.s3.amazonaws.com/skadi/"


class TILESET():

    def get_path(self, lat, lng):
        lat_filename = "N"
        if lat < 0:
            lat_filename = "S"
        lat_filename += str(abs(lat)).zfill(2) 

        lng_filename = "E"
        if lng < 0:
            lng_filename = "W"

        lng_filename += str(abs(lng)).zfill(3) 
        filename = lat_filename+lng_filename+'.hgt.gz'
        return lat_filename+"/"+filename

    def get_elevation(self, lat, lng):
        
        tlat = math.floor(lat)
        tlng = math.floor(lng)

        r = requests.get(base_url + self.get_path(tlat, tlng))
        buffer = gzip.decompress(r.content)

        tile = HGT(buffer,[tlat, tlng])
        return tile.get_elevation([lat, lng])

    def get_elevations(self, wkt):
        
        bbox = wkt.bounds
        minlng = math.floor(bbox[0])
        minlat = math.floor(bbox[1])
        maxlng = math.floor(bbox[2])
        maxlat = math.floor(bbox[3])
        elevs = {}
        lngs = []
        lats = []

        for tlng in range(minlng,maxlng+1,1):
            for tlat in range(minlat,maxlat+1,1):
                r = requests.get(base_url + self.get_path(tlat, tlng))
                buffer = gzip.decompress(r.content)
                tile = HGT(buffer,[tlat, tlng])
                olngs, olats, els = tile.get_elevations(wkt)
                elevs.update(els)
                lngs.extend(olngs)
                lats.extend(olats)

        flngs = np.sort(np.unique(lngs))
        flats = np.sort(np.unique(lats))

        result = []
        for tlng in flngs:
            for tlat in flats:
                pkey = str(tlng) + '_' + str(tlat)
                if pkey in elevs:
                    result.append(elevs[pkey])
                else:
                    result.append('null')
        return flngs.tolist(), flats.tolist(), result
