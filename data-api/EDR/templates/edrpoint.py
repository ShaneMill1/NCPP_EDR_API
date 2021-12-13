import json
import copy
from EDR.templates import parameter_map as pm
from numpy import ndarray
import numpy
from EDR.provider.metadata import MetadataProvider

EXCLUDE_PARAMETERS = ['projection_y_coordinate_bnds','projection_y_coordinate','projection_x_coordinate',
                      'projection_x_coordinate_bnds', 'lambert_azimuthal_equal_area']

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

#
#    "x" : { "values": [-10.1] },
#    "y" : { "values": [ -40.2] },
#    "t" : { "values": ["2013-01-13T11:12:20Z"] }

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

range_json = {
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


def set_parameter_vals(pt, data, p_names, config, dataset):
    if config is not None:
        mp_ = MetadataProvider(config)
    if len(p_names) == 0:
        p_names = list(data.keys())
                
    for p in data.keys():
        if (not (p in EXCLUDE_PARAMETERS)) and (p in p_names):
            if ('attrs' in data[p]) and ('parameter_template_discipline_category_number' in data[p]['attrs']):
                try:
                    pt['parameters'][p] = mp_.set_grib_detail(str(data[p]['attrs']['parameter_template_discipline_category_number'][1]),str(data[p]['attrs']['parameter_template_discipline_category_number'][2]),str(data[p]['attrs']['parameter_template_discipline_category_number'][3]),data[p]['attrs']['long_name'],data[p]['attrs']['units'])
                except:
                    pt['parameters'][p] = pm.getDef(p)
            else:
                if config is None:
                    pt['parameters'][p] = pm.getDef(p)
                else:
                    if p in config['datasets'][dataset]['parameters']:
                        parts = config['datasets'][dataset]['parameters'][p][0].split('/')
                        if parts[2] == 'bufr4':
                            pt['parameters'][p] = mp_.get_buf4_detail(parts[2],parts[3],parts[4],None)
                        elif parts[2] == 'grib2':
                            pt['parameters'][p] = mp_.get_grib_detail(parts[2],parts[3],parts[4],None)
                    else:
                        print ( p)
                        pt['parameters'][p] = pm.getDef(p)                    
    return pt

def set_range_vals(pt, data, t_key, pList, p_names):

    pIndex = 0
    if len(p_names) == 0:
        p_names = pList

    for p in pList:
        if (not (p in EXCLUDE_PARAMETERS)) and (p in p_names):
            pt['ranges'][p] = copy.deepcopy(range_json)
            pt['ranges'][p]['axisNames'] = [t_key]

            npa = numpy.array(data[p]['data'], dtype=float, copy=True)
            pt['ranges'][p]['shape'] = npa.shape
            nda = numpy.ndarray(npa.shape, buffer=npa)

            pt['ranges'][p]['values'] = nda.flatten('C').tolist()
        pIndex += 1
    return pt


def get_point(coords, data, t_data, x_key, y_key, t_key, p_names, config=None, dataset=None):

    pt = copy.deepcopy(pt_template)
    if x_key == 'lon_0':
        pt = set_axis_vals(pt, 'x', float(coords[x_key]['data']) - 180.0)
    else:
        pt = set_axis_vals(pt, 'x', float(coords[x_key]['data']))
    pt = set_axis_vals(pt, 'y', float(coords[y_key]['data']))

    if t_key is not None:
        if len(t_data) > 1:
            pt['domain']['domainType'] = 'PointSeries'
        pt = set_axis_vals(pt, 't', t_data)

    if (x_key.find('lon') > -1) or (x_key.find('projection_x_coordinate') > -1) or (x_key.find('xgrid') > -1):
        pt['domain']['referencing'].append(ref['GeographicCRS'])
    if (t_key is not None):
        pt['domain']['referencing'].append(ref['ISO8601'])

    if 'variable' in coords:
        pt = set_parameter_vals(pt, coords['variable']['data'], p_names, config, dataset)
        pt = set_range_vals(pt, data, 't', coords['variable']['data'], p_names)
    else:
        pt = set_parameter_vals(pt, data, p_names, config, dataset)
        pt = set_range_vals(pt, data, 't', data.keys(), p_names)

    return pt
