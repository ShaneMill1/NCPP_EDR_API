import json
import copy
from EDR.templates import parameter_map as pm
from EDR.provider.metadata import MetadataProvider


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
       },
       "ISO8601":
       {
           "coordinates": ["t"],
           "system": {
               "type": "TemporalRS",
               "calendar": "Gregorian"
           }
       }
       }

range = {
    "type": "NdArray",
    "dataType": "float",
    "axisNames": [],
    "shape": [],
    "values": []
}


def set_axis_vals(pt, axis_key, axis_vals):

    pt['domain']['axes'][axis_key] = copy.deepcopy(axis_template)

    if type(axis_vals) is list:
        pt['domain']['axes'][axis_key]['values'] = axis_vals
    else:
        pt['domain']['axes'][axis_key]['values'].append(axis_vals)

    return pt


def parameter_map(pt, p_name, config):
    if config is not None:
        if type(config['datasets']['metar']['parameters'][p_name][0]) is str:
            mp_ = MetadataProvider(config)
            parts = config['datasets']['metar']['parameters'][p_name][0].split('/')
            pt['parameters'][p_name] =  mp_.get_buf4_detail(parts[2],parts[3],parts[4],None)
        else:
            pt['parameters'][p_name] = config['datasets']['metar']['parameters'][p_name][0]

def set_parameter_vals(pt, p_names, config):

    if len(p_names) == 0:
        p_names = config['datasets']['metar']['parameters']
    for p in p_names:
        parameter_map(pt, p, config)
      

    return pt


def set_range_vals(pt, data, t_key, p_names, config):

    if len(p_names) == 0:
        p_names = config['datasets']['metar']['parameters']

    for p in p_names:
        pt['ranges'][p] = copy.deepcopy(range)
        pt['ranges'][p]['values'] = []
        pt['ranges'][p]['shape'] = [len(data)]
        pt['ranges'][p]['axisNames'] = [t_key]


    for m in data:
        for p in p_names:
            val = get_item(m, config['datasets']['metar']['parameters'][p][2])
            if val is not None:
                if config['datasets']['metar']['parameters'][p][3] == 'float':
                    pt['ranges'][p]['values'].append(float(val))
                elif config['datasets']['metar']['parameters'][p][3] == 'int':
                    pt['ranges'][p]['values'].append(int(val))
                    pt['ranges'][p]['dataType'] = 'int'
                else:
                    pt['ranges'][p]['values'].append(val)
                    pt['ranges'][p]['dataType'] = 'string'
            else:
                pt['ranges'][p]['values'].append("Null")

    return pt

def get_item(metarob, item):
    try:
        if item == 'temp':
            return metarob.temp.value()
        elif item == 'dewpt':     
            return metarob.dewpt.value()
        elif item == 'wind_speed':     
            return metarob.wind_speed.value()
        elif item == 'wind_gust':     
            return metarob.wind_gust.value()
        elif item == 'wind_dir':     
            return metarob.wind_dir.value()
        elif item == 'vis':     
            return metarob.vis.value()
        elif item == 'press':     
            return metarob.press.value()
        elif item == 'mslp':     
            return metarob.press_sea_level.value()
        elif item == 'raw_ob':     
            return metarob.code        
        elif item == 'icao_id':     
            return metarob.station_id
        else:
            return None
    except AttributeError:
        return None

def get_point(data, lat, lon, p_names, config):

    t_data = {}
    t_data['timesteps'] = {"data": [], "dims": [
        "t"], "attrs": {"units": "ISO8601"}}
    for m in data:
        t_data['timesteps']['data'].append(m.time.isoformat())

    pt =copy.deepcopy(pt_template)
    pt = set_axis_vals(pt, 'x', float(lon))
    pt = set_axis_vals(pt, 'y', float(lat))

    pt['domain']['domainType'] = 'PointSeries'
    pt = set_axis_vals(pt, 't', t_data['timesteps']['data'])

    pt['domain']['referencing'].append(ref['GeographicCRS'])
    pt['domain']['referencing'].append(ref['ISO8601'])

    pt = set_parameter_vals(pt, p_names, config)
    pt = set_range_vals(pt, data, 't', p_names, config)

    return pt

