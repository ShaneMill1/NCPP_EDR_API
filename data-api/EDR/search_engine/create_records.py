#!/opt/conda/bin/python
import argparse
import glob
import json
from collections import Counter
import os
from bs4 import BeautifulSoup
import requests
from datetime import datetime

def extract_metadata():
   try:
      os.makedirs('/EDR/search_engine/records')
   except:
      pass
   path=sorted(glob.glob('/data/collections/*/*/*_collection.json'))
   index_list=list()
   coll_list=list()
   for p in path:
      index_list.append(p.split('/')[4]+'_'+p.split('/')[5])
      with open(p) as f:
         coll_list.append(json.load(f))
   #need this to carry the instance
   catalog = { i : coll_list[idx] for idx,i in enumerate(index_list) }
   #for loop through list of models
   for colinst in catalog:
      if 'ndfd' in colinst:
         pass
      else:
      #for loop for list of collections
         try:
            instance=colinst.split('_')[0]
            model=colinst.split('_')[3]+'_'+colinst.split('_')[4]
            forecast_range=list()
            for c in catalog[colinst]:
               collection_id=c['collection_name']
               keywords=c['long_name']+[c['level_type']]
               param_ln=c['long_name']
               param_ids=c['parameters']
               param_level_type=c['level_type']
               forecast_range=['nan','nan']
               level_list=list()
               lvl_id=''
               level_list=list()
               for k in c.keys():
                  if 'forecast_time' in k and 'ln' not in k:
                     forecast_range=[c[k][0].strip("'"),c[k][-1].strip("'")]
                  if 'lv' in k and 'ln' not in k:
                     l_list=c[k]
                     for l in l_list:
                        if 'e' in l:
                           try:
                              z_array=l.split('e')
                              d=float(z_array[0])*10**int(z_array[1])
                              level_list.append(d)
                           except:
                              pass
                        else:
                           level_list.append(l)
                     lvl_id=k
               record_dir='/EDR/search_engine/records/'
               url_base='http://data-api-c.mdl.nws.noaa.gov/OGC-EDR-API/collections/automated_'
               data_url=url_base+collection_id+'/instances/'+instance+'?f=application/json'
               create_xml_record_file(data_url,collection_id,param_ids,keywords,instance,forecast_range,param_ln,param_level_type,lvl_id,level_list,record_dir)
         except:
            print('the specified collection metadata '+colinst+' is not formatted correctly')
   return


def metar_items_metadata():
   record_dir='/EDR/search_engine/records/'
   #record_dir='./'
   collection_id='metar_tgftp'
   data_location='https://tgftp.nws.noaa.gov/data/observations/metar/stations/'
   url_base='http://data-api-c.mdl.nws.noaa.gov/OGC-EDR-API/collections/'
   org_name='National Weather Service'
   abstract="A METAR Aviation Routine Weather Report is a WMO and ICAO  Code form representing an observation of a set of weather elements, made at a specific airport and time, authorised and managed by the WMO member state, and measured according to declared ICAO and WMO standards"
   keywords=['METAR','Aviation','Hourly','Weather','Observation','Temperature','Dew point','Precipitation amount', 'Visibility','Cloud amount', 'Cloud type', 'Cloud height', 'Weather runway']
   tgftp_home=requests.get(data_location)
   soup=BeautifulSoup(tgftp_home.text,'html.parser')
   soup_formatted=(soup.prettify())
   cycles=['latest','00Z','01Z','02Z','03Z','04Z','05Z','06Z','07Z','08Z','09Z','10Z','11Z','12Z','13Z','14Z','15Z','16Z','17Z','18Z','19Z','20Z','21Z','22Z','23Z','24Z']
   encoding=['raw','decoded']
   for tr in soup.find_all('tr'):
      try:
         loc=tr.find_all('td')[0].find_all('a')[0].text
         time=tr.find_all('td')[1].text
         current_year=str(datetime.today().year)
         last_year=str(datetime.today().year-1)
         file_year=time.split('-')[2].split(' ')[0]
         if current_year == file_year:
            if ".TXT" in loc:
               for c in cycles:
                  for e in encoding:
                     item=loc.split('.')[0]+'_'+c+'_'+e
                     #else:
                     #   item=loc.split('.')[0]+'_'+c
                     data_url=url_base+collection_id+'/items/'+item+'?f=application/json'
                     create_new_xml_record_file(data_url,collection_id,item,record_dir,org_name,abstract,keywords)
                     print(item+' created')
      except:
         pass
   for c in cycles:
      item='collective_'+c
      data_url=url_base+collection_id+'/items/'+item+'?f=application/json'
      create_new_xml_record_file(data_url,collection_id,item,record_dir,org_name,abstract,keywords)
      print(item+' created')
   return


def create_new_xml_record_file(data_url,collection_id,item,record_dir,org_name,abstract,keywords):
   with open(record_dir+item+'_'+collection_id+'.xml','w') as xml:
      xml.write('<?xml version="1.0" encoding="UTF-8"?>\n')
      xml.write('<csw:Record xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gco="http://www.isotc211.org/2005/gco" '\
            +'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:swe="http://www.opengis.net/swe/2.0" '\
            +'xmlns:ows="http://www.opengis.net/ows" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dct="http://purl.org/dc/terms/" '\
            +'xmlns:om="http://www.opengis.net/om/2.0" xmlns:gml="http://www.opengis.net/gml/3.2" '\
            +'xsi:schemaLocation="http://www.opengis.net/om/2.0 http://schemas.opengis.net/om/2.0/observation.xsd '\
            +' http://www.opengis.net/swe/2.0 http://schemas.opengis.net/sweCommon/2.0/swe.xsd" gml:id="'+collection_id+'_'+item+'">\n')      
      xml.write('  <dc:identifier>'+collection_id+'_'+item+'</dc:identifier>\n')
      xml.write('  <dc:URI>'+data_url+'</dc:URI>\n')
      #xml.write('  <gmd:contact>\n')
      #xml.write('     <gmd:organisationName xsi:type="gmd:PT_FreeText_PropertyType>\n')
      #xml.write('        <gco:CharacterString>'+org_name+'</gco:CharacterString>\n')
      #xml.write('     </gmd:organisationName>\n')
      #xml.write('  </gmd:contact>\n')
      xml.write('  <gmd:identificationInfo>\n')
      xml.write('     <gmd:MD_DataIdentification>\n')
      xml.write('        <gmd:abstract>\n')
      xml.write('           <gco:CharacterString>"'+abstract+'"</gco:CharacterString>\n')
      xml.write('        </gmd:abstract>\n')
      for k in keywords:
         xml.write('        <gmd:descriptiveKeywords>\n')
         xml.write('           <gmd:keyword xsi:type="gmd:PT_FreeText_PropertyType">\n')
         xml.write('              <gco:CharacterString>'+k+'</gco:CharacterString>\n')
         xml.write('           </gmd:keyword>\n')
         xml.write('        </gmd:descriptiveKeywords>\n')
      xml.write('     </gmd:MD_DataIdentification>\n')
      xml.write('  </gmd:identificationInfo>\n')
      xml.write('</csw:Record>')
   return





def create_xml_record_file(data_url,collection_id,param_ids,keywords,instance,forecast_range,param_ln,param_level_type,lvl_id,level_list,record_dir):
   kw_list=list()
   with open(record_dir+instance+'_'+collection_id+'.xml','w') as xml:
      xml.write('<?xml version="1.0" encoding="UTF-8"?>\n')
      xml.write('<csw:Record xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '\
            +'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:swe="http://www.opengis.net/swe/2.0" '\
            +'xmlns:ows="http://www.opengis.net/ows" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dct="http://purl.org/dc/terms/" '\
            +'xmlns:om="http://www.opengis.net/om/2.0" xmlns:gml="http://www.opengis.net/gml/3.2" '\
            +'xsi:schemaLocation="http://www.opengis.net/om/2.0 http://schemas.opengis.net/om/2.0/observation.xsd '\
            +' http://www.opengis.net/swe/2.0 http://schemas.opengis.net/sweCommon/2.0/swe.xsd" gml:id="'+instance+'_'+collection_id+'">\n')
      xml.write('  <dc:identifier>'+instance+'_'+collection_id+'</dc:identifier>\n')
      xml.write('  <dc:URI>'+data_url+'</dc:URI>\n')
      xml.write('  <om:OM_Observation gml:id="'+instance+'_'+collection_id+'">\n')
      if len(forecast_range)>0:
             xml.write('    <om:phenomenonTime>\n'\
                      +'      <gml:TimePeriod>\n'\
                      +'        <gml:beginPosition>'+forecast_range[0]+'</gml:beginPosition>\n'\
                      +'        <gml:endPosition>'+forecast_range[1]+'</gml:endPosition>\n'\
                      +'      </gml:TimePeriod>\n'\
                      +'    </om:phenomenonTime>\n')
      xml.write('    <om:result xsi:type="swe:DataRecordPropertyType">\n')
      for idx,p in enumerate(param_ids):
         xml.write('      <swe:DataRecord>\n')
         xml.write('        <swe:field name="'+param_ln[idx]+'" gml:id="'+p+'">\n')
         xml.write('          <swe:Quantity definition="">\n')
         xml.write('            <swe:uom code=""/>\n')
         xml.write('            <swe:value>'+param_ln[idx]+'</swe:value>\n')
         xml.write('          </swe:Quantity>\n')
         xml.write('        </swe:field>\n')
         xml.write('      </swe:DataRecord>\n')
      if lvl_id != '':
         xml.write('      <swe:DataRecord>\n')
         xml.write('        <swe:field name="'+param_level_type+'" gml:id="'+lvl_id+'">\n')
         xml.write('          <swe:Quantity definition="">\n')
         xml.write('            <swe:uom code=""/>\n')
         for l in level_list:
            xml.write('            <swe:value>'+str(l)+'</swe:value>\n')
         xml.write('          </swe:Quantity>\n')
         xml.write('        </swe:field>\n')
         xml.write('      </swe:DataRecord>\n')
      xml.write('    </om:result>\n')
      xml.write('  </om:OM_Observation>\n')
      xml.write('</csw:Record>')
   return


if __name__ == "__main__":
   extract_metadata()
   #commented out metar because it takes at least 40 min to run, so copying db  
   metar_items_metadata()
