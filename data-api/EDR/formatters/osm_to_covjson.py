import json
import copy


t_template = {
    "type": "Coverage",
    "domain": {
        "type": "Domain",
        "domainType": "Trajectory",
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


def set_parameter_vals(tr,way_name,way_id, params):
    tr["parameters"] = {}
    
    for p in params:
        tr["parameters"][p] = {
        "type" : "Parameter",
        "description" : {
            "en": way_name
        },
        "unit" : {
            "label": {
            "en": "node id"
            },
            "symbol": {
            "value": "1",
            "type": "https://wiki.openstreetmap.org/wiki/Node"
            }
        },
        "observedProperty" : {
            "id" : "https://nominatim.openstreetmap.org/lookup?osm_ids=W"+str(way_id),
            "label" : {
            "en": way_name
            }
        }
        }

    return tr


def set_range_vals(tr, data, p_names, config):


    if len(p_names) == 0:
        p_names = config['datasets']['osm_highways']['parameters']

    for p in p_names:
        tr['ranges'][p] = copy.deepcopy(range)
        tr['ranges'][p]['values'] = []
        tr['ranges'][p]['shape'] = [len(data.nodes)]
        tr['ranges'][p]['axisNames'] = ['composite']
        tr['ranges'][p]['dataType'] = 'int'
        for node in data.nodes:
            tr['ranges'][p]['values'].append(node.id)

    return tr


def get_output(data, params, config):

    tr =copy.deepcopy(t_template)
    coords = []
    for node in data.nodes:
        coords.append([float (node.lon), float(node.lat)])
    tr = set_axis_vals(tr, 'composite', coords)
    tr['domain']['axes']['composite']['dataType'] = 'tuple'
    tr['domain']['axes']['composite']['coordinates'] = ["x","y"]
    tr['domain']['domainType'] = 'Trajectory'

    tr['domain']['referencing'].append(ref['GeographicCRS'])

    w_name = ""
    if 'name' in data.tags:
        w_name = data.tags['name']
    tr = set_parameter_vals(tr, w_name, data.id, params)
    tr = set_range_vals(tr, data, params, config)

    return tr
