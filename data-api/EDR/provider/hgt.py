import math
import struct
from shapely.geometry import Point, Polygon


def avg(v1, v2, f):
    return v1 + (v2 - v1) * f

class HGT():

    def __init__(self, buffer, swlat_lng):
        self._buffer = buffer
        self._swlat_lng = swlat_lng

        if len(buffer) == (12967201 * 2):
            self._resolution = 1
            self._size = 3601
        elif len(buffer) == (1442401 * 2):
            self._resolution = 3
            self._size = 1201
        else:
            print("Unknown tile format (1 arcsecond and 3 arcsecond supported).")

    def nearest_neighbour(self, row, col):
        return self._rowCol(round(row), round(col))


    def bilinear(self, row, col):
        row_low = math.floor(row)
        row_hi = row_low + 1
        row_frac = row - row_low
        col_low = math.floor(col)
        col_hi = col_low + 1
        col_frac = col - col_low
        v00 = self.row_col(row_low, col_low)
        v10 = self.row_col(row_low, col_hi)
        v11 = self.row_col(row_hi, col_hi)
        v01 = self.row_col(row_hi, col_low)
        v1 = avg(v00, v10, col_frac)
        v2 = avg(v01, v11, col_frac)
        return avg(v1, v2, row_frac)

    def get_elevation(self, lat_lng):
        size = self._size - 1    
        ll = lat_lng
        row = (ll[0] - self._swlat_lng[0]) * size
        col = (ll[1] - self._swlat_lng[1]) * size

        if (row < 0) or (col < 0) or (row > size) or (col > size):
            print("Latitude/longitude is outside tile bounds (row=" + row + ", col=" + col + "; size=" + size)

        return self.bilinear(row, col)

    def get_elevations(self, wkt):
        size = self._size - 1
        elevs = {}
        lngs= []
        lats = []
        tile_poly = Polygon([(self._swlat_lng[1],self._swlat_lng[0]),(self._swlat_lng[1],self._swlat_lng[0]+1),(self._swlat_lng[1]+1,self._swlat_lng[0]+1),(self._swlat_lng[1]+1,self._swlat_lng[0]),(self._swlat_lng[1],self._swlat_lng[0])]) 
        inter  = wkt.intersection(tile_poly)

        min_row = math.floor((inter.bounds[1] - float(self._swlat_lng[0])) * size)
        min_col = math.floor((inter.bounds[0] - float(self._swlat_lng[1])) * size)
        max_row = math.ceil((inter.bounds[3] - float(self._swlat_lng[0])) * size)
        max_col = math.ceil((inter.bounds[2] - float(self._swlat_lng[1])) * size)
        if (max_row > 3600):
            max_row = 3600
        if (max_col > 3600):
            max_col = 3600
        if (min_row < 0):
            min_row = 0
        if (min_col < 0):
            min_col = 0
        
        
        for col in range(min_col, max_col, 1):
            for row in range(min_row, max_row, 1):
                lng = (col / size) + self._swlat_lng[1]
                lat = (row / size) + self._swlat_lng[0]
                if wkt.contains(Point(lng, lat)):
                    elevs[str(lng) + "_" + str(lat)] = self.bilinear(row, col)
                    lngs.append(lng)
                    lats.append(lat)

        
        return lngs, lats, elevs

    def row_col(self, row, col):
        size = self._size
        offset = ((size - row - 1) * size + col) * 2

        return struct.unpack_from('>h', self._buffer, offset)[0]
