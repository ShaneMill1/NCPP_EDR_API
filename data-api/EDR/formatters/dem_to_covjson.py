import json
import copy

ply_template = {
    "type": "Coverage",
    "domain": {
        "type": "Domain",
        "domainType": "Grid",
        "axes": {
        },
        "referencing": []
    },
    "parameters": {
    "DEM": {
      "type" : "Parameter",
      "description": {
      	"en": "Topographic data generated from NASA's Shuttle Radar Topography Mission global coverage (except high latitudes), 30 meter resolution in land areas"
      },
      "unit" : {
        "label": {
          "en": "m"
        },
        "symbol": {
          "value": "m",
          "type": "http://www.opengis.net/def/uom/UCUM/"
        }
      },
      "observedProperty" : {
        "id" : "http://vocab.nerc.ac.uk/standard_name/height_above_mean_sea_level/",
        "label" : {
          "en": "Elevation"
        }
      }
    }

    },
    "ranges": {
    }
}


pt_template = {
    "type": "Coverage",
    "domain": {
        "type": "Domain",
        "domainType": "Point",
        "axes": {

        },
        "referencing": []
    },
    "parameters": {
    "DEM": {
      "type" : "Parameter",
      "description": {
      	"en": "Topographic data generated from NASA's Shuttle Radar Topography Mission global coverage (except high latitudes), 30 meter resolution in land areas"
      },
      "unit" : {
        "label": {
          "en": "m"
        },
        "symbol": {
          "value": "m",
          "type": "http://www.opengis.net/def/uom/UCUM/"
        }
      },
      "observedProperty" : {
        "id" : "http://vocab.nerc.ac.uk/standard_name/height_above_mean_sea_level/",
        "label" : {
          "en": "Elevation"
        }
      }
    }

    },
    "ranges": {
    }
}

axis_template = {"values": []}


ref = {"GeographicCRS":
       {
           "coordinates": ["x", "y"],
           "system": {
               "type": "GeographicCRS",
               "id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
           }
       }
       }

range_json = {
    "type": "NdArray",
    "dataType": "float",
    "axisNames": [],
    "shape": [],
    "values": []
}

point_json = {
    "type": "NdArray",
    "dataType": "float",
    "values": []
}

def set_axis_vals(pt, lons, lats):

    pt['domain']['axes']['x'] = copy.deepcopy(axis_template)
    if type(lons) is list:
        pt['domain']['axes']['x']['values'] = lons
    else:
        pt['domain']['axes']['x']['values'].append(lons)

    pt['domain']['axes']['y'] = copy.deepcopy(axis_template)
    if type(lats) is list:
        pt['domain']['axes']['y']['values'] = lats
    else:
        pt['domain']['axes']['y']['values'].append(lats)

    return pt



def set_range_vals(pt, data, xsize, ysize):




    if (xsize is None) and (ysize is None):
        pt['ranges']['DEM'] = copy.deepcopy(point_json)
        pt['ranges']['DEM']['values'].append(data)
    else:
        pt['ranges']['DEM'] = copy.deepcopy(range_json)
        pt['ranges']['DEM']['axisNames'] = ['x', 'y']
        pt['ranges']['DEM']['shape'] = [xsize, ysize]

        pt['ranges']['DEM']['values'] = data

    return pt


def get_polygon(lons, lats, data):

    pt = copy.deepcopy(ply_template)
    pt = set_axis_vals(pt, lons, lats)
    pt['domain']['referencing'].append(ref['GeographicCRS'])
    pt = set_range_vals(pt, data, len(lons), len(lats))    
    return pt

def get_point(lons, lats, data):
    pt = copy.deepcopy(pt_template)
    pt = set_axis_vals(pt, lons, lats)    
    pt['domain']['referencing'].append(ref['GeographicCRS'])
    pt = set_range_vals(pt, data, None, None) 

    return pt