import sqlite3
import logging
import os
import json
import EDR.util as util
from EDR.provider.base import ProviderConnectionError, ProviderQueryError
from EDR.provider import InvalidProviderError, units
import copy

LOGGER = logging.getLogger(__name__)

VERSION = '0.0.1'
HEADERS = {
    'Content-type': 'application/json',
    'X-Powered-By': 'Weather on the Web {}'.format(VERSION)
}

PARAMETER_TEMPLATE = {
                "type": "Parameter",
                "description": {
                    "en": ""
                },
                "unit": "",
                "observedProperty": {
                    "id": "",
                    "label": {
                        "en": ""
                    }
                }
            }

class MetadataProvider(object):

    def __init__(self,config):


        self.db = config['metadata']['registry_db']
        self.base_url = config['server']['url'].rstrip('/')

    def __response_parameter(self, register, table, identifier):
        """
        """

        result = copy.deepcopy(PARAMETER_TEMPLATE)
        for row_data in self.dataDB:
            result['description']['en'] = row_data[5]
            result['unit'] = units.unit(self.base_url, row_data[7])
            result['observedProperty']['id'] = self.base_url + "/" + register + "/" + table + "/" + identifier
            result['observedProperty']['label']['en'] = row_data[5]


        return result


    def __load(self):
        """
        Private method for loading spatiallite,
        get the table structure and dump geometry

        :returns: sqlite3.Cursor
        """

        if (os.path.exists(self.db)):
            conn = sqlite3.connect(self.db)
        else:
            raise InvalidProviderError


        cursor = conn.cursor()

        return cursor

    def query(self, headers, args, register=None, table=None, identifier=None, detail=None):
        """
        """
        output = ""

        
        
        if register is None:
            if 'search' in args: 
                search_term = "%" + args.get('search') + "%"
                output = self.search(search_term,register)
        elif register == "uom" and table == "UCUM":
            if not identifier == None:
                output = units.ucum(identifier)
        else:
            if 'search' in args: 
                search_term = "%" + args.get('search') + "%"
                output = self.search(search_term,register)
            elif register == "bufr4":
                if table == "a":
                    output = {}
                elif table == "b":
                    output = {}

                elif table == "c":
                    output = {}

                elif table == "d":
                    output = self.get_buf4_detail(register, table, identifier, detail)

                elif table == "codeflag":
                    output = {}
                else:
                    output = {
                        "tables" : [
                            {
                            "name":"BUFR Table A - Data category", 
                            "description":"http://www.wmo.int/pages/prog/www/WMOCodes/WMO306_vI2/LatestVERSION/WMO306_vI2_BUFR_TableA_en.pdf",
                            "link": self.base_url +"/bufr4/a"
                            },
                            {
                            "name":"BUFR/CREX Table B - Classification of elements and tables", 
                            "description":"http://www.wmo.int/pages/prog/www/WMOCodes/WMO306_vI2/LatestVERSION/WMO306_vI2_BUFRCREX_TableB_en.pdf",
                            "link": self.base_url +"/bufr4/b"
                            },
                            {
                            "name":"BUFR Table C - Data description operators", 
                            "description":"http://www.wmo.int/pages/prog/www/WMOCodes/WMO306_vI2/LatestVERSION/WMO306_vI2_BUFR_TableC_en.pdf",
                            "link": self.base_url +"/bufr4/c"
                            },
                            {
                            "name":"BUFR Table D - List of common sequences", 
                            "description":"http://www.wmo.int/pages/prog/www/WMOCodes/WMO306_vI2/LatestVERSION/WMO306_vI2_BUFR_TableD_en.pdf",
                            "link": self.base_url +"/bufr4/d"
                            },
                            {
                            "name":"Code and Flag Tables associated with BUFR/CREX Table B", 
                            "description":"http://www.wmo.int/pages/prog/www/WMOCodes/WMO306_vI2/LatestVERSION/WMO306_vI2_BUFRCREX_CodeFlag_en.pdf",
                            "link": self.base_url +"/bufr4/codeflag"
                            }                          
                        ]
                    }   
            elif register == "grib2":
                if table == "codeflag":

                    output = self.get_grib_detail(register, table, identifier, detail)
                elif table == "template":
                    output = {}
                else:
                    output = {
                        "tables" : [
                            {
                            "name":"Templates - Grid definition, Product definition, Data representation and Data", 
                            "description":"http://www.wmo.int/pages/prog/www/WMOCodes/WMO306_vI2/LatestVERSION/WMO306_vI2_GRIB2_Template_en.pdf",
                            "link": self.base_url +"/grib2/template"
                            },
                            {
                            "name":"Code and Flag Tables", 
                            "description":"http://www.wmo.int/pages/prog/www/WMOCodes/WMO306_vI2/LatestVERSION/WMO306_vI2_GRIB2_CodeFlag_en.pdf",
                            "link": self.base_url +"/grib2/codeflag"
                            }                          
                        ]
                    }                    
            elif register == "proj4":
                if table.find("conus") > -1:
                    output = '+proj=lcc +lat_1=25 +lat_2=25 +lat_0=25 +lon_0=265 +x_0=0 +y_0=0 +a=6371200 +b=6371200 +units=m +no_defs'
                elif table.find("ukv") > -1:
                    output = '+proj=laea +lon_0=-2.5 +lat_0=54.9 +ellps=GRS80 +x_0=0.0 +y_0=0.0 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs'
                elif table.find("guam") > -1 or table.find("hawaii") > -1:
                    output = '+proj=merc +lon_0=0 +lat_ts=20 +x_0=0 +y_0=0 +a=6371200 +b=6371200 +units=m +no_defs'
                elif table.find("alaska") > -1:
                    output = '+proj=stere +lat_0=90 +lat_ts=60 +lon_0=210 +k=90 +x_0=0 +y_0=0 +a=6371200 +b=6371200 +units=m +no_defs'

        headers_ = HEADERS.copy()
        if not register == "proj4":
            output = json.dumps(output)
        else:
            headers_['Content-type'] = 'text/plain'


        return headers_, 200, output

    def search(self, search_term, register):

        cursor = self.__load()
        if register == "bufr4":
            sql_query = 'select distinct "bufr4" as type, d.CategoryOfSequences_en as category, d.Title_en as title, d.FXY1 as id1,  d.FXY2 as id2, d.ElementName_en as element, d.Status as status from BUFR_31_0_0_TableD_en as d, BUFRCREX_31_0_0_TableB_en as b where LOWER(d.Title_en) like "{}" or LOWER(d.ElementName_en) like "{}";'.format(search_term,search_term)
        elif register == "grib2":
            sql_query = 'select distinct "grib2" as type, Title_en as category, SubTitle_en as title, "" as id1, CodeFlag as id2, MeaningParameterDescription_en as element, Status as status  from GRIB2_22_0_0_CodeFlag_en where Title_en == "Code table 4.2 - Parameter number by product discipline and parameter category" and LOWER(MeaningParameterDescription_en) like "{}";'.format(search_term)
        else:
            sql_query = 'select distinct "bufr4" as type, d.CategoryOfSequences_en as category, d.Title_en as title, d.FXY1 as id1,  d.FXY2 as id2, d.ElementName_en as element, d.Status as status from BUFR_31_0_0_TableD_en as d, BUFRCREX_31_0_0_TableB_en as b where LOWER(d.Title_en) like "{}" or LOWER(d.ElementName_en) like "{}" union select distinct "grib2" as type, Title_en as category, SubTitle_en as title, "" as id1, CodeFlag as id2, MeaningParameterDescription_en as element, Status as status  from GRIB2_22_0_0_CodeFlag_en where Title_en == "Code table 4.2 - Parameter number by product discipline and parameter category" and LOWER(MeaningParameterDescription_en) like "{}";'.format(search_term,search_term,search_term)

        LOGGER.debug('SQL Query:{}'.format(sql_query))

        self.dataDB = cursor.execute(sql_query)
        results = []
        for row_data in self.dataDB:
            result = {}
            result['type'] = row_data[0]
            result['category'] = row_data[1]
            result['title'] = row_data[2]
            result['element'] = row_data[5]
            if row_data[0] == "bufr4":
                result['details'] = self.base_url + "/metadata/bufr4/d/" + row_data[3] + "-" + row_data[4]
            elif row_data[0] == "grib2":
                codes = [int(s) for s in row_data[2].replace(':','').split() if s.isdigit()]
                result['details'] = self.base_url + "/metadata/grib2/codeflag/4.2-" + str(codes[0]) + "-" + str(codes[1]) + "-" + row_data[4]
            results.append(result)
        return results

    def get_buf4_detail(self, register, table, identifier, detail):
        """

        """

        LOGGER.debug('Get item from Sqlite')

        cursor = self.__load()

        LOGGER.debug('Got cursor from DB')
        parts = identifier.split("-")
        
        if table == "d":
            sql_query = 'select d.Category, d.CategoryOfSequences_en, d.FXY1, d.Title_en, d.FXY2, d.ElementName_en, d.Status, b.ClassName_en, b.FXY, b.CREX_Unit from BUFR_31_0_0_TableD_en as d LEFT OUTER JOIN BUFRCREX_31_0_0_TableB_en as b on d.FXY2=FXY where d.FXY1="{}" and d.FXY2="{}";'.format(parts[0], parts[1])

        LOGGER.debug('SQL Query:{}'.format(sql_query))
        LOGGER.debug('Identifier:{}'.format(identifier))

        self.dataDB = cursor.execute(sql_query)
        result = {}
        
        for row_data in self.dataDB:
            if row_data[9] == "C":
                inunits = "Cel"
            else:
                inunits = row_data[9]
            if detail is None:
                result['description'] = {}
                result['description']['en'] = row_data[5]
                result['unit'] = units.unit(self.base_url, inunits)
                result['observedProperty'] = {}
                result['observedProperty']['id'] = self.base_url + "/metadata/" + register + "/" + table + "/" + identifier
                result['observedProperty']['label'] = {}
                result['observedProperty']['label']['en'] = row_data[5]            
            else:
                result['Title'] = row_data[3]
                result['Product_discipline'] = {'name':row_data[7],'value':row_data[4]}
                result['Parameter_category'] = {'name':row_data[1],'value':row_data[0]}
                result['Code_Flag'] = row_data[2]
                result['Parameter_Description'] = row_data[5]
                result['Units'] = units.unit(self.base_url, inunits)
                result['Status'] = row_data[6]


        return result


    def get_grib_detail(self, register, table, identifier, detail):
        """

        """

        LOGGER.debug('Get item from Sqlite')

        cursor = self.__load()

        LOGGER.debug('Got cursor from DB')
        parts = identifier.split('-')
        dbtable = ""
        if table == "codeflag":
            dbtable = "GRIB2_22_0_0_CodeFlag_en"

        sql_query = 'select * from {} where Title_en == "Code table {} - Parameter number by product discipline and parameter category" and CodeFlag = "{}" and SubTitle_en like "Product discipline {} - % products, parameter category {}:%";'.format(dbtable, parts[0], parts[3], parts[1], parts[2])

        LOGGER.debug('SQL Query:{}'.format(sql_query))
        LOGGER.debug('Identifier:{}'.format(identifier))

        self.dataDB = cursor.execute(sql_query)
        result = {}
        for row_data in self.dataDB:
            if detail is None:
                result = self.set_grib_detail(register, table, identifier,row_data[5],row_data[7])
            else:
                tparts = row_data[2].split(',')
                pd = tparts[0].split(' - ')
                pc = tparts[1].split(': ')    
                result['Title'] = row_data[1]
                result['Product_discipline'] = {'name':pd[1],'value':parts[1]}
                result['Parameter_category'] = {'name':pc[1],'value':parts[2]}
                result['Code_Flag'] = row_data[3]
                result['Parameter_Description'] = row_data[5]
                result['Units'] = row_data[7]
                result['Status'] = row_data[8]


        return result


    def set_grib_detail(self, register, table, identifier, desc, inunits):
        """

        """
        result = {}
        result['description'] = {}
        result['description']['en'] = desc
        result['unit'] = units.unit(self.base_url, inunits)
        result['observedProperty'] = {}
        result['observedProperty']['id'] = self.base_url + "/metadata/" + register + "/" + table + "/" + identifier
        result['observedProperty']['label'] = {}
        result['observedProperty']['label']['en'] = desc                   


        return result


    def __repr__(self):
        return '<SQLiteProvider> {}'.format(self.db)






