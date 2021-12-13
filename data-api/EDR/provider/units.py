
import json
import xmltodict
import requests
from EDR.cache_logic import ucum_cache

def get_unit_def():
    global ucum_cache
    if ucum_cache is None:
        r = requests.get('http://unitsofmeasure.org/ucum-essence.xml')
        xmlcontent = xmltodict.parse(r.content)
        
        ucum_cache = xmlcontent
    return ucum_cache     



def unit(base_url, symbol):

    unit = {"label":{"en": ""},"symbol": {"value": "","type": "http://www.opengis.net/def/uom/UCUM/"}}
    if symbol is None:
        unit['label']['en'] = ""
        unit['symbol']['value'] = ""
        unit['symbol']['type'] = ""
    else:
        content = ucum(symbol)

        if "name" in content['unit']:
            unit['label']['en'] = content['unit']['name']
            unit['symbol']['value'] = content['unit']['printSymbol']
            unit['symbol']['type'] = base_url + "/metadata/uom/UCUM/" + symbol
        elif "name" in content['base unit']:
            unit['label']['en'] = content['base unit']['name']
            unit['symbol']['value'] = content['base unit']['printSymbol']
            unit['symbol']['type'] = base_url + "/metadata/uom/UCUM/" + symbol
        else:
            unit['label']['en'] = "unknown"
            unit['symbol']['value'] = symbol
            unit['symbol']['type'] = ""
    return unit    


def ucum(symbol):

    content = get_unit_def()
    unit = {}
    unit['unit'] = {}
    unit['base unit'] = {}
    unit['prefix'] = {}
    unit['type'] = None
    for uccm_unit in content['root']['unit']:
        try:
            if uccm_unit['@Code'] == symbol:
                unit['unit']= uccm_unit
                unit['type'] = "unit"
        except TypeError as te:
            print (te)   
    if unit['type'] == None:
        for uccm_unit in content['root']['base-unit']:
            try:
                if uccm_unit['@Code'] == symbol:
                    unit['base unit'] = uccm_unit
                    unit['type'] = "base unit"
            except TypeError as te:
                print (te)                
    if unit['type'] == None and len(symbol) == 1:
        for uccm_unit in content['root']['prefix']:
            try:
                if uccm_unit['@Code'] == symbol:
                    unit['prefix'] = uccm_unit
                    unit['type'] = "prefix"
            except TypeError as te:
                print (te)   

    if unit['type'] == None and len(symbol) > 1:
        fst = symbol[:1]
        lst = symbol[1:]
        for uccm_unit in content['root']['prefix']:
            try:
                if uccm_unit['@Code'] == fst:
                    unit['prefix'] = uccm_unit
                    unit['type'] = "prefix"
            except TypeError as te:
                print (te)                
        for uccm_unit in content['root']['unit']:
            try:
                if uccm_unit['@Code'] == lst and (not (unit['type'] is None)):
                    unit['unit'] = uccm_unit
                    unit['type'] = unit['type']+ "+unit"
            except TypeError as te:
                print (te)
        if unit['type'] == "prefix":
            for uccm_unit in content['root']['base-unit']:
                try:
                    if uccm_unit['@Code'] == lst and (not (unit['type'] is None)):
                        unit['unit'] = uccm_unit
                        unit['type'] = unit['type']+ "+base unit"
                except TypeError as te:
                    print (te)                       
             
    return unit    
