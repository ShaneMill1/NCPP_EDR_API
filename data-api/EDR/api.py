from datetime import datetime, timedelta
import json
import logging
import os
import copy
import shapely.wkt
from shapely.geometry import box
from jinja2 import Environment, FileSystemLoader
from shapely.geometry import Polygon, Point
from EDR.log import setup_logger
from EDR.provider import load_provider
from EDR.formatters import FORMATTERS, load_formatter, format_output
from EDR.provider.base import ProviderConnectionError, ProviderQueryError
from EDR.templates.open_api import OPENAPI
from EDR.util import style_html
import EDR.isodatetime.parsers as parse
import pprint
pp = pprint.PrettyPrinter(indent=4)
import urllib.parse
from urllib.request import urlopen
import glob
from flask import request as rq
import dateutil.parser as dp
from bs4 import BeautifulSoup
import requests
import s3fs

VERSION = '0.0.1'
LOGGER = logging.getLogger(__name__)

TEMPLATES = '{}{}templates'.format(os.path.dirname(
    os.path.realpath(__file__)), os.sep)

HEADERS = {
    'X-Powered-By': 'Environment Data Retrieval API {}'.format(VERSION)
}


class API(object):
    """API object"""

    def __init__(self, config):
        """
        constructor

        :param config: configuration dict

        :returns: `EDR.API` instance
        """

        self.config = config
        self.config['server']['url'] = self.config['server']['url'].rstrip('/')
        self.server = config['server']['url']

        if 'templates' not in self.config['server']:
            self.config['server']['templates'] = TEMPLATES
        setup_logger(self.config['logging'])

    def link_template(self, links, inpath):
        in_parts = inpath[1:].split('/')
        links.append({
            "href": self.server + inpath + "?f=application%2Fjson",
            "rel": "self",
            "type": "application/json",
            "title": in_parts[-1]  + " document as json"
        })
        links.append({
            "href": self.server + inpath + "?f=application%2Fx-yaml",
            "rel": "alternate",
            "type": "application/x-yaml",
            "title": in_parts[-1] + " document as yaml"
        })
        links.append({
            "href": self.server + inpath + "?f=text%2Fxml",
            "rel": "alternate",
            "type": "text/xml",
            "title": in_parts[-1] + " document as xml"
        })
        links.append({
            "href": self.server + inpath + "?f=text%2Fhtml",
            "rel": "alternate",
            "type": "text/html",
            "title": in_parts[-1] + " document as html"
        })
        return links


    def root(self, headers, args):
        """
        Provide API

        :param headers: dict of HTTP headers
        :param args: dict of HTTP request parameters

        :returns: tuple of headers, status code, content
        """
        if args.get('f'):
           content_type=args.get('f')
           try:
              content_type=urllib.parse.unquote(content_type)
           except:
              pass
        else:
           try:
              content_type=headers['Accept']
           except:
              content_type = 'text/html'
        headers = {}
        fo = format_output.FormatOutput(self.config, ["/","/api","/metadata","/conformance","/groups","/collections"])
        output={}
        output['title']='NWS Environmental Data Retrieval API Server'
        output['description']='OGC-API EDR Implementation developed by National Weather Service - Meteorology Development Lab'
        output['links']=fo.create_links(False)
        if content_type is not None:

            if content_type.find('html') > -1:
                output = _render_j2_template(self.config, 'root.html', output)
                content_type = 'text/html'
            elif content_type.find('xml') > -1:
                output = fo.get_xml(output)
                content_type = 'application/xml'
            elif (content_type.find('yml') > -1) or (content_type.find('yaml') > -1):
                output = fo.get_yaml(output)
                content_type = 'application/x-yaml'
            else:
                output = fo.get_json(output)
                content_type = 'application/json'
        else:
            content_type = 'text/html'
            output = _render_j2_template(self.config, 'root.html', output)
 
        headers['Content-Type'] = content_type

        return headers, 200, output




    def api(self, headers, args):
        """
        Provide OpenAPI document

        :param headers: dict of HTTP headers
        :param args: dict of HTTP request parameters

        :returns: tuple of headers, status code, content
        """
        open_api_ = OPENAPI(self.config['server']['url'])
        content_type = args.get('f')
        try:
           content_type=urllib.parse.unquote(content_type)
        except:
           try:
              content_type=headers['Accept']
           except:
              content_type = 'text/html'

        output = ''
        if content_type is not None:
            if content_type.find('json') > -1:
                output = open_api_.get_json()
            elif content_type.find('html') > -1:
                output =  open_api_.get_html()
            elif content_type.find('xml') > -1:
                output = open_api_.get_xml()
            elif (content_type.find('yml')) or (content_type.find('yaml') > -1):
                output = open_api_.get_yaml()
        else:
            content_type = 'text/html'
            output =  open_api_.get_html()

        headers_ = HEADERS.copy()
        headers_['Content-type'] = content_type

        return headers_, 200, output

    def api_conformance(self, headers, args):
        """
        Provide conformance definition

        :param headers: dict of HTTP headers
        :param args: dict of HTTP request parameters

        :returns: tuple of headers, status code, content
        """

        headers_ = HEADERS.copy()

        formats = ['json', 'html','application/json','text/html','application%2Bjson','text%2Bhtml']

        format_ = args.get('f')
        try:
           format_=urllib.parse.unquote(format_)
        except:
           try:
              content_type=headers['Accept']
           except:
              content_type = 'text/html'

        if format_ is not None and format_ not in formats:
            exception = {
                'code': 'InvalidParameterValue',
                'description': 'Invalid format'
            }
            LOGGER.error(exception)
            return headers_, 400, json.dumps(exception)

        conformance = {'conformsTo': ["http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",\
                           "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/collections",\
                           "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/oas3",\
                           "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/html",\
                           "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/geojson",\
                           "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/core",\
                           "http://www.opengis.net/spec/ogcapi-edr-1/1.0/conf/core"]}
        try:
           if 'json' in content_type:
              headers_['Content-type']='application/json'
           if (format_.find('html') > -1):  # render
              headers_['Content-type'] = 'text/html'
              content = _render_j2_template(self.config, 'conformance.html',
                                          conformance)
              return headers_, 200, content
        except:
           pass

        return headers_, 200, json.dumps(conformance)

    def describe_group(self, headers, args, subpath=None):
        output = {}
        fo = format_output.FormatOutput(self.config, ['/groups'])
        output['links'] = fo.create_links(False)
        content_type = args.get('f')
        try:
           content_type=urllib.parse.unquote(content_type)
        except:
           try:
              content_type=headers['Accept']
           except:
              content_type = 'text/html'
        
        group_links = []
        cdescrip = None
        headers = {}
        group_links, cdescrip = self.group_metadata(subpath)
        template_name = "group.html"

        if cdescrip == None:
            fo = format_output.FormatOutput(self.config, group_links)
            output['members'] = fo.create_links(True)
        #
        # Temporary hack until we better understand this code
        else:
            fo = format_output.FormatOutput(self.config, ["/collections"])
            co_descrip = fo.collections_description(cdescrip, False)
            output['members'] = []
            for co_link in co_descrip:
                if isinstance(co_link, list):
                    for aDict in co_link:
                        if aDict['href'].find('collections') > -1:
                            output['members'].append(aDict)
#        else:
#            fo = format_output.FormatOutput(self.config, ["/collections"])
#            co_descrip = fo.collections_description(cdescrip, False)
#            output['members'] = []
#            for co_link in co_descrip:
#                if co_link[3]['href'].find('collection') > -1:
#                    output['members'].append(co_link)

        if content_type is not None:

            if content_type.find('html') > -1:
                output = _render_j2_template(self.config,template_name,output)
                content_type = 'text/html'
            elif content_type.find('xml') > -1:
                output = fo.get_xml(output)
                content_type = 'application/xml'
            elif (content_type.find('yml') > -1) or (content_type.find('yaml') > -1):
                output = fo.get_yaml(output)
                content_type = 'application/x-yaml'
            else:
                output = fo.get_json(output)
                content_type = 'application/json'
        else:
            content_type = 'text/html'
            output = _render_j2_template(self.config,template_name,output)


        headers['Content-Type'] = content_type
        return headers, 200, output

    def group_metadata(self, subpath):
        
        group_links = []
        cdescrip = None
        if not subpath is None:
            groupids = subpath.split("/")
            groupid = groupids[-1]
        if subpath == None:
            for group in self.config['groups']:
                if self.config['groups'][group]['type'] == 'group':
                    group_links.append('/groups/'+group)
        else:
            group_links, cdescrip = self.build_group_links(subpath, groupids,  groupid, group_links)
        
        return group_links, cdescrip

    def build_group_links(self, subpath, groupids, groupid, group_links):
        cdescrip = None
        if not (groupid in self.config['groups']):
            cdescrip = 'summary£'+groupid       
        elif self.config['groups'][groupid]['type'].find('group') > -1:
            for child in self.config['groups'][groupid]['children']:
                group_links.append('/groups/'+subpath+"/"+child)
        else:
            cdescrip = 'summary£'+self.config['datasets'][groupid]["provider"]["name"]     

        return group_links, cdescrip

    def list_items(self, headers, args, collection):
       fo = format_output.FormatOutput(self.config, ["/collections/"+collection])
       content_type = args.get('f')
       output={}
       items=self.set_items(collection) 
       output['items'] = items
       output['name'] = collection
       output['title'] = collection.replace('_',' ')
       output['parameters'] = fo.get_parameter_list(collection)
       headers={}
       if content_type is not None:
            if content_type.find('html') > -1:
                headers['Content-Type'] = 'text/html'
                output = _render_j2_template(self.config,"collection.html",output)
            elif content_type.find('xml') > -1:
                headers['Content-Type'] = 'application/xml'
                output = fo.get_xml(output)
            elif (content_type.find('yml') >-1) or (content_type.find('yaml') > -1):
                headers['Content-Type'] = 'application/x-yaml'
                output = fo.get_yaml(output)
            else:
                output = fo.get_json(output)
                headers['Content-Type'] = 'application/json'
       else:
            headers['Content-Type'] = 'text/html'
            output = _render_j2_template(self.config,"collection.html",output)

       return headers, 200, output




    def list_identifers(self, headers, args, collection):
        output = {}
        fo = format_output.FormatOutput(self.config, ["/collections/"+collection])
        output['links'] = fo.create_links(False)
        output['instances'] = []
        content_type = args.get('f')
        try:
           content_type=urllib.parse.unquote(content_type)
        except:
           try:
              content_type=headers['Accept']
           except:
              content_type = 'text/html'
        
        instance = self.set_instance_type(collection)
        
        output['instances'] = instance
        output['name'] = collection
        output['title'] = collection.replace('_',' ')
        output['parameters'] = fo.get_parameter_list(collection)
        headers = {}
        if content_type is not None:
            if content_type.find('html') > -1:
                headers['Content-Type'] = 'text/html'
                output = _render_j2_template(self.config,"collection.html",output)
            elif content_type.find('xml') > -1:
                headers['Content-Type'] = 'application/xml'
                output = fo.get_xml(output)
            elif (content_type.find('yml') >-1) or (content_type.find('yaml') > -1):
                headers['Content-Type'] = 'application/x-yaml'
                output = fo.get_yaml(output)
            else:
                output = fo.get_json(output)
                headers['Content-Type'] = 'application/json'
        else:
            headers['Content-Type'] = 'text/html'
            output = _render_j2_template(self.config,"collection.html",output)

        return headers, 200, output

    def instance_label(self, headers, args, collection):
        output = {}
        fo = format_output.FormatOutput(self.config, ["/collections/"+collection])
        output['links'] = fo.create_links(False)
        path=rq.path
        output['members']=self.link_template( [], path+"/instances")
        content_type = args.get('f')
        try:
           content_type=urllib.parse.unquote(content_type)
        except:
           try:
              content_type=headers['Accept']
           except:
              content_type = 'text/html'
        
        output['title'] = collection.replace('_',' ')
        headers = {}
        if content_type is not None:
            if content_type.find('html') > -1:
                headers['Content-Type'] = 'text/html'
                output = _render_j2_template(self.config,"collection.html",output)
            elif content_type.find('xml') > -1:
                headers['Content-Type'] = 'application/xml'
                output = fo.get_xml(output)
            elif (content_type.find('yml') >-1) or (content_type.find('yaml') > -1):
                headers['Content-Type'] = 'application/x-yaml'
                output = fo.get_yaml(output)
            else:
                output = fo.get_json(output)
                headers['Content-Type'] = 'application/json'
        else:
            headers['Content-Type'] = 'text/html'
            output = _render_j2_template(self.config,"collection.html",output)

        
        return headers, 200, output


    def item_label(self, headers, args, collection):
        output = {}
        fo = format_output.FormatOutput(self.config, ["/collections/"+collection])
        output['links'] = fo.create_links(False)
        path=rq.path
        output['members']=self.link_template( [], path+"/items")
        content_type = args.get('f')
        try:
           content_type=urllib.parse.unquote(content_type)
        except:
           try:
              content_type=headers['Accept']
           except:
              content_type = 'text/html'

        output['title'] = collection.replace('_',' ')
        headers = {}
        if content_type is not None:
            if content_type.find('html') > -1:
                headers['Content-Type'] = 'text/html'
                output = _render_j2_template(self.config,"collection.html",output)
            elif content_type.find('xml') > -1:
                headers['Content-Type'] = 'application/xml'
                output = fo.get_xml(output)
            elif (content_type.find('yml') >-1) or (content_type.find('yaml') > -1):
                headers['Content-Type'] = 'application/x-yaml'
                output = fo.get_yaml(output)
            else:
                output = fo.get_json(output)
                headers['Content-Type'] = 'application/json'
        else:
            headers['Content-Type'] = 'text/html'
            output = _render_j2_template(self.config,"collection.html",output)

        return headers, 200, output


    def set_default_instance_type(self, collection):
        fn = ""
        for ds in self.config['datasets']:
            if collection.find(self.config['datasets'][ds]['name']) > -1:
                if self.config['datasets'][ds]['provider']['type'] == 'obs':
                    fn = 'raw'
                if self.config['datasets'][ds]['provider']['type'] == 'annex_3':
                    if self.config['datasets'][ds]['provider']['name'] == 'tca':
                       fn='tca'
                    if self.config['datasets'][ds]['provider']['name'] == 'vaa':
                       fn='vaa'
                if self.config['datasets'][ds]['provider']['type'] == 'tgftp':
                   if self.config['datasets'][ds]['provider']['name'] == 'metar_tgftp':
                      fn='metar_tgftp'
                if self.config['datasets'][ds]['provider']['type'] == 'wifs_png':
                   if self.config['datasets'][ds]['provider']['name'] == 'wifs_png':
                      fn='wifs_png'
                if self.config['datasets'][ds]['provider']['type'] == 'waa_active':
                   if self.config['datasets'][ds]['provider']['name'] == 'wwa_active':
                      fn='wwa_active'
                elif self.config['datasets'][ds]['provider']['type'] == 'model_file':                    
                   try:            
                      fn = self.config['datasets'][ds]['provider']['data_source']+'coord_info.json'
                   except:
                      fn = 'latest'           
                else:
                    fn = 'latest'        
        return fn

    def set_items(self, collection):
        items=[]
        if collection == 'tca':
            data_location=self.config['datasets'][collection]['provider']['data_source']
            tca_item_list=glob.glob(data_location+'/*NHC*')
            for t in tca_item_list:
               t_str=t.split('/')[-1:][0]
               t_str=t_str.split('.')[0]+'_'+t.split('.')[1]
               items.append(self.item_desc(collection, t_str, t_str + '',''+t_str+'','/collections/'+collection))
        if collection == 'metar_tgftp':
            cycles=self.config['datasets'][collection]['provider']['cycle']
            encoding=self.config['datasets'][collection]['provider']['data_representation']
            #metar_endpoints=['stations','t1t2']
            #test t1t2 only
            metar_endpoints=['t1t2']
            for m in metar_endpoints:
               if m == 'stations':
                  data_location=self.config['datasets'][collection]['provider']['data_source']+'/'+m
                  tgftp_home=requests.get(data_location)
                  soup=BeautifulSoup(tgftp_home.text,'html.parser')
                  soup_formatted=(soup.prettify())
                  tgftp_home.close()
                  item_list=list()
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
                                    t_str=loc.split('.')[0]+'_'+c+'_'+e
                                    items.append(self.item_desc(collection, t_str, t_str + '',''+t_str+'','/collections/'+collection))
                     except:
                        pass
               if m =='t1t2':
                  data_location=self.config['datasets'][collection]['provider']['t1t2_data_source']+'?C=M;O=D'
                  tgftp_home=requests.get(data_location)
                  soup=BeautifulSoup(tgftp_home.text,'html.parser')
                  soup_formatted=(soup.prettify())
                  tgftp_home.close()
                  item_list=list()
                  for tr in soup.find_all('tr'):
                     try:
                        loc=tr.find_all('td')[0].find_all('a')[0].text
                        loc=loc.replace('.txt','')
                        loc=loc.replace('.','_')
                        loc=loc[:-1]
                        if 'Parent' not in loc:
                           items.append(self.item_desc(collection, loc, loc + '',''+loc+'','/collections/'+collection))
                     except:
                        print('element does not contain t1t2 info')
            #need to trim list because of memory issues in browser
            items=items[0:20]
        if collection == 'wifs_png':
           cycles=self.config['datasets'][collection]['provider']['cycle']
           items_map={'SWH-H_North_Atlantic_US': 'PGAE05_KKCI',
                      'SWH-I_North_Pacific_US': 'PGBE05_KKCI',
                      'SWH-M_North_Pacific_US': 'PGDE29_KKCI',
                      'SWH-A_Americas_US': 'PGEE05_KKCI',
                      'SWH-F_North_Pacific_US': 'PGGE05_KKCI',
                      'SWH-B1_Americas_AFI_US': 'PGIE05_KKCI'}
           for key, value in items_map.items():
              for c in cycles:
                 t_str=key+'_'+c
                 items.append(self.item_desc(collection, t_str, t_str + '',''+t_str+'','/collections/'+collection))
        if 'wwa_active' in collection:
           source_url=self.config['datasets']['wwa_active']['provider']['data_source']
           wwa_home=requests.get(source_url)
           soup=BeautifulSoup(wwa_home.text,'html.parser')
           entries=soup.find_all('entry')
           for e in entries:
              cap_event=e.find_all('cap:event')[0].string.replace(' ','_').lower()
              if 'wwa_active_'+cap_event == collection:
                 cap_effective=e.find_all('cap:effective')[0].string
                 item_name=cap_event+'_'+cap_effective
                 try:
                    loc_id=e.find_all('value')[-1].string.split('.')[2]
                    item_name=cap_event+'_'+loc_id+'_'+cap_effective
                 except:
                    item_name=cap_event+'_'+cap_effective
                 extent={}
                 fips6=e.find_all('value')[0].string
                 UGC=e.find_all('value')[1].string
                 extent['FIPS6']=fips6
                 extent['UGC']=UGC
                 items.append(self.item_desc(collection, item_name, item_name + '',''+item_name+'','/collections/'+collection, extent)) 
        
        schema_list=[rq.url.replace('items?f=application%2Fjson','locations/capatom.xsl'),rq.url.replace('items?f=application%2Fjson','locations/dst_check.xsl')]
        items[0]['schema']=schema_list
        return items



    def set_instance_type(self, collection):
        instance = []
        fn = self.set_default_instance_type(collection)
        if fn.find('coord_info.json') > -1:
            with open(fn) as json_file:
                iid = json.load(json_file)['folder']
                instance.append(self.instance_desc(collection, 'latest', 'Latest model run', 'The ' + collection + ' collection for the latest available model run', "/collections/"+collection))
                instance.append(self.instance_desc(collection, iid, iid + ' model run', 'The ' + collection + ' collection for the '+iid+' model run', "/collections/"+collection))
        elif fn == 'raw':
            instance.append(self.instance_desc(collection, 'raw', 'Raw values', 'The raw data values for the ' + collection + ' collection', "/collections/"+collection))
            instance.append(self.instance_desc(collection, 'qcd', 'Quality controlled values', 'The Quality controlled values for the ' + collection + ' collection', "/collections/"+collection))
        elif fn == 'tca':
            data_location=self.config['datasets'][collection]['provider']['data_source']
            tca_instance_list=glob.glob(data_location+'/*NHC*')
            for t in tca_instance_list:
               t_str=t.split('/')[-1:][0]
               t_str=t_str.split('.')[0]+'_'+t.split('.')[1]
               instance.append(self.instance_desc(collection, t_str, t_str + 'instance of Tropical Cyclone Advisories','The '+t_str+' instance of the Tropical Cyclone Advisories','/collections/'+collection))
        elif fn == 'vaa':
            data_location_v=self.config['datasets'][collection]['provider']['data_source']
            tca_instance_list_v=glob.glob(data_location_v+'/FV*')
            for t_v in tca_instance_list_v:
               t_str_v=t_v.split('/')[-1:][0]
               t_str_v=t_str_v.split('.')[0]+'_'+t_v.split('.')[1]
               instance.append(self.instance_desc(collection, t_str_v, t_str_v + 'instance of Volcanic Ash Advisories','The '+t_str_v+' instance of the Volcanic Ash Advisories','/collections/'+collection))
        else:
            if 'automated' in collection:
               instance_model=['gfs_100']
               data_location=self.config['datasets'][collection]['provider']['data_source']
               for m in instance_model:
                  fs=s3fs.S3FileSystem()
                  instance_cycle=fs.glob(data_location+'/'+m+'/*')
                  for c in instance_cycle:
                     c_name=os.path.basename(c)
                     instance.append(self.instance_desc(collection,c_name,c_name+' values', 'The '+c+' values for the automated ' + m + ' collection', "/collections/automated_"+m))
            else:
               instance.append(self.instance_desc(collection, 'latest', 'Latest values', 'The latest values for the ' + collection + ' collection', "/collections/"+collection))
        return instance



    def describe_automated_collections(self, headers, args, dataset, identifier):
        fo = format_output.FormatOutput(self.config, ["/collections/"+dataset+"/"+identifier])
        output = fo.create_links(False)
        content_type = args.get('f')
        try:
           content_type=urllib.parse.unquote(content_type)
        except:
           try:
              content_type=headers['Accept']
           except:
              content_type = 'text/html'
        output = fo.automated_collection_desc(dataset,True)
        output['name'] = dataset
        output['instance'] = identifier
        headers = {}
        try:
            if content_type is not None:
                if content_type.find('html') > -1:
                    headers['Content-Type'] = 'text/html'
                    output = _render_j2_template(self.config,"instance.html",output)
                elif content_type.find('xml') > -1:
                    headers['Content-Type'] = 'application/xml'
                    output = fo.get_xml(output)
                elif (content_type.find('yml') >-1) or (content_type.find('yaml') > -1):
                    headers['Content-Type'] = 'application/x-yaml'
                    output = fo.get_yaml(output)
                else:
                    output = fo.get_json(output)
                    headers['Content-Type'] = 'application/json'
            else:
                headers['Content-Type'] = 'text/html'
                output = _render_j2_template(self.config,"instance.html",output)
        except:
            output = _render_j2_template(self.config,'error.html',{})

        return headers, 200, output   

    def describe_item(self, headers, args, dataset, item):
        headers = {}
        content_type = args.get('f')
        fo = format_output.FormatOutput(self.config, ["/collections/"+dataset+"/"+item])
        output = fo.items_description(dataset, True)
        try:
            if content_type is not None:
                if content_type.find('html') > -1:
                    headers['Content-Type'] = 'text/html'
                    output = _render_j2_template(self.config,"instance.html",output)
                elif content_type.find('xml') > -1:
                    headers['Content-Type'] = 'application/xml'
                    output = fo.get_xml(output)
                elif (content_type.find('yml') >-1) or (content_type.find('yaml') > -1):
                    headers['Content-Type'] = 'application/x-yaml'
                    output = fo.get_yaml(output)
                else:
                    output = fo.get_json(output)
                    headers['Content-Type'] = 'application/json'
            else:
                headers['Content-Type'] = 'text/html'
                output = _render_j2_template(self.config,"instance.html",output)
        except:
            output = _render_j2_template(self.config,'error.html',{})
        return headers, 200, output



    def describe_collection(self, headers, args, dataset, identifier=''):
        fo = format_output.FormatOutput(self.config, ["/collections/"+dataset+"/"+identifier])
        content_type = args.get('f')
        try:
           content_type=urllib.parse.unquote(content_type)
        except:
           try:
              content_type=headers['Accept']
           except:
              content_type = 'text/html'
        if dataset=='wwa_active':
           output=fo.wwa_active_all({},False)
        else:
           output = fo.collections_description(dataset, True)
        try:
           if output['id']=='vaa' or output['id'] == 'tca':
              for idx,o in enumerate(output['links']):
                 if identifier not in o['href']:
                    output["links"][idx]['href']=o['href'].replace('missing',identifier) 
        except:
           pass
        output['name'] = dataset
        output['instance'] = identifier
        headers = {}
        try:
            if content_type is not None:
                if content_type.find('html') > -1:
                    headers['Content-Type'] = 'text/html'
                    output = _render_j2_template(self.config,"instance.html",output)
                elif content_type.find('xml') > -1:
                    headers['Content-Type'] = 'application/xml'
                    output = fo.get_xml(output)
                elif (content_type.find('yml') >-1) or (content_type.find('yaml') > -1):
                    headers['Content-Type'] = 'application/x-yaml'
                    output = fo.get_yaml(output)
                else:
                    output = fo.get_json(output)
                    headers['Content-Type'] = 'application/json'
            else:
                headers['Content-Type'] = 'text/html'
                output = _render_j2_template(self.config,"instance.html",output)
        except:
            output = _render_j2_template(self.config,'error.html',{})

        return headers, 200, output

    def describe_collections(self, headers, environ, args, dataset=None):
        """
        Provide feature collection metadata

        :param headers: dict of HTTP headers
        :param args: dict of HTTP request parameters

        :returns: tuple of headers, status code, content
        """
        ipparts = environ['REMOTE_ADDR'].split('.')
        if len(headers.getlist("X-Forwarded-For")) > 0:
            ipparts = headers.getlist("X-Forwarded-For")[0].split('.')

        met_ip = False
        if (ipparts[0] == '151') and (ipparts[1] == '170') or environ['REMOTE_ADDR'] == '127.0.0.1':
            met_ip = True
        fo = format_output.FormatOutput(self.config, ["/collections"], met_ip)
        content_type = args.get('f')
        try:
           content_type=urllib.parse.unquote(content_type)
        except:
           try:
              content_type=headers['Accept']
           except:
              content_type = 'text/html'
        
        output = fo.collections_description("all", True)

        headers = {}
        try:
            if content_type is not None:
                if content_type.find('html') > -1:
                    headers['Content-Type'] = 'text/html'
                    output = _render_j2_template(self.config,"collections.html",output)
                elif content_type.find('xml') > -1:
                    headers['Content-Type'] = 'application/xml'
                    output = fo.get_xml(output)
                elif (content_type.find('yml') >-1) or (content_type.find('yaml') > -1):
                    headers['Content-Type'] = 'application/x-yaml'
                    output = fo.get_yaml(output)
                else:
                    output = fo.get_json(output)
                    headers['Content-Type'] = 'application/json'
            else:
                headers['Content-Type'] = 'text/html'
                output = _render_j2_template(self.config,'collections.html',output)
        except:
            output = _render_j2_template(self.config,'error.html',{})

        return headers, 200, output

    def get_feature(self, headers, args, dataset, identifier):
        """
        Get a feature

        :param headers: dict of HTTP headers
        :param args: dict of HTTP request parameters
        :param dataset: collection name

        :returns: tuple of headers, status code, content
        """
        headers_ = HEADERS.copy()
        if dataset=='tca' or dataset=='metar_tgftp' or dataset=='wifs_png' or 'wwa_active' in dataset:
           coords=''; qtype=''; valid_loc=True;time_range=None;z_value=None;params=None
           outputFormat=rq.args['f']
           headers_, http_code, output = self.get_provider(dataset, qtype, coords, time_range, z_value, params, identifier, outputFormat)
           return headers_, http_code, output
        elif len(args) > 0:
            coords, qtype, valid_loc = self.process_coords(args, dataset)
            try:
               timerange = args.get('datetime')
            except:
               timerange = args.get('date-time')
            try:
               timerange=urllib.parse.unquote(timerange)
            except:
               try:
                  content_type=headers['Accept']
               except:
                  content_type = 'text/html'
            
            params = self.get_params(args)
            time_range = None
            output = None
            if timerange is not None:
                if 'ndfd_xml' in dataset or 'goes' in dataset or 'himawari' in dataset:
                   if '/' in timerange:
                      time_range=timerange.split('/')
                   else:
                      time_range=timerange
                else:
                   try:
                      if timerange.find("/") > -1:
                         time_range = parse.TimeIntervalParser().parse(timerange)
                      else:
                         time_range = parse.TimeIntervalParser().parse(timerange+"/"+timerange)
                   except:
                      pass
            z_value = args.get('z')
            if z_value:
               try: 
                  z_value=urllib.parse.unquote(z_value)
               except:
                  pass
            if z_value == None:
                z_value = args.get('Z')
                try:
                   z_value=urllib.parse.unquote(z_value)
                except:
                   pass

            if valid_loc:
                outputFormat = args.get('f')
                try:
                   outputFormat=urllib.parse.unquote(outputFormat)
                except:
                   pass
                headers_, http_code, output = self.get_provider(dataset, qtype, coords, time_range,
                                                                z_value, params, identifier, outputFormat)
            else:
                exception = {
                    'code': '400',
                    'description': 'Data requested is outside of the area covered by the datasource '
                }
                LOGGER.error(exception)
                headers_['Content-Type'] = 'application/json'
                return headers_, 400, json.dumps(exception)
        else:
            raise ProviderQueryError()

        return headers_, http_code, output 

    def get_provider(self, dataset, qtype, coords, time_range, z_value, params, identifier, outputFormat):
        headers_ = HEADERS.copy()
        try:
           outputFormat=outputFormat.split('/')[1]
        except:
           outputFormat=outputFormat
        try:
            p = load_provider(dataset, self.config)
            output, delete_file = p.query(dataset, qtype, coords, time_range, z_value, params, identifier, outputFormat)
            if delete_file != 'no_delete':
               delete_files=delete_file.split('.')[0]+'*'
               if os.path.isdir(delete_files):
                  for f in glob.glob(delete_files):
                     os.remove(f)
               else:
                  os.remove(delete_file)
            if outputFormat == 'json':
                headers_['Content-type'] = 'application/json'
            elif outputFormat == 'html':
                raise ProviderConnectionError
            elif outputFormat == 'xml' or outputFormat=='cap':
                headers_['Content-type'] = 'text/xml'
            elif outputFormat == 'raw':
                headers_['Content-type'] = 'text/raw'

            return headers_, 200, output

        except ProviderConnectionError:
            exception = {
                'code': 'NoApplicableCode',
                'description': 'connection error (check logs)'
            }
            headers_['Content-Type'] = 'application/json'
            LOGGER.error(exception)
            return headers_, 500, json.dumps(exception) 

    def get_params(self, args):
        params = []
        if args.get('parameter-name') is not None:
            param_arg=args.get('parameter-name')
            try:
               param_arg=urllib.parse.unquote(param_arg)
            except:
               pass
            params = param_arg.split(",")
            # handle leading and trailing spaces in the parameter list
            for ploop in range(0,len(params)):
                params[ploop] = params[ploop].strip()        
        return params


    def process_coords(self, args, dataset):

        bbox = self.get_bbox(dataset)
        valid_loc = False
        coords = []
        if args.get('coords') is not None:
            coord_args=args.get('coords')
            try:
               coord_args=urllib.parse.unquote(coord_args)
            except:
               pass
            ##block to deal with LINESTRING:
            if 'LINESTRING' in coord_args:
               if 'LINESTRINGM' in coord_args:
                  coord_args=coord_args.replace('M','')
                  coord_args=coord_args.replace('LINESTRING(','')
                  coord_args=coord_args.replace(')','')
                  coord_args=coord_args.split(',')
                  qtype='linestringm'
                  for c in coord_args:
                     coords.append(tuple(c.split(' ')))
               if 'LINESTRINGZ' in coord_args:
                  if 'LINESTRINGZM' in coord_args:
                     coord_args=coord_args.replace('ZM','')
                     coord_args=coord_args.replace('LINESTRING(','')
                     coord_args=coord_args.replace(')','')
                     coord_args=coord_args.split(',')
                     qtype='linestringzm'
                     for c in coord_args:
                        coords.append(tuple(c.split(' ')))
                  else:
                     coord_args=coord_args.replace('Z','')
                     coord_args=coord_args.replace('LINESTRING(','')
                     coord_args=coord_args.replace(')','')
                     coord_args=coord_args.split(',')
                     qtype='linestringz'
                     for c in coord_args:
                        coords.append(tuple(c.split(' ')))
            try:
               wkt = shapely.wkt.loads(coord_args)
               qtype = wkt.type.lower()
            except:
               pass
            if qtype == 'point':
                coords.append(wkt.x)
                coords.append(wkt.y)
            elif qtype == 'multipoint':
                for x in wkt.wkt[12:-1].split(','):
                    coords.append(x.strip().split(' '))                    
            elif qtype == 'polygon':
                for cloop in range(0,len(wkt.exterior.xy[0])):
                    coord = [wkt.exterior.xy[0][cloop],wkt.exterior.xy[1][cloop]]
                    coords.append(coord)
            elif qtype=='linestring':
               wkt = shapely.wkt.loads(coord_args)
               qtype = wkt.type.lower()
               coords=list(wkt.coords)
            #if bbox.contains(wkt):
            #    valid_loc = True
        #handling of wkt in this code block is not correct. need to investigate
        valid_loc=True
        return coords, qtype, valid_loc 

    def get_bbox(self, dataset):
        bbox_str = []
        if dataset in self.config['datasets']:
            bbox_str = self.config['datasets'][dataset]['extent']['spatial'].split(",")
        else:
            try:
               bbox_str = self.config['datasets'][dataset.split("_")[0]+'_'+dataset.split("_")[1]]['extent']['spatial'].split(",")
            except:
               try:
                  bbox_str = self.config['datasets'][dataset.split("_")[0]]['extent']['spatial'].split(",")
               except:
                  bbox_str=[]
        try:
           bbox = box(float(bbox_str[0]), float(bbox_str[1]), float(bbox_str[2]), float(bbox_str[3]))
        except:
           bbox=''
        return bbox


    def instance_desc(self, collection, iid, title, description, link_path):
        desc = {}
        desc['id'] = iid
        desc['title'] = title
        desc['description'] = description
        fo = format_output.FormatOutput(self.config, [link_path+"/instances/"+iid])
        #This line is slowing down the generation of available instances
        #desc['extent'] = fo.collections_description('extent£'+collection,True)
        desc['links'] = fo.create_links(False)
        
        return desc

    def item_desc(self, collection, iid, title, description, link_path, extent={}):
        desc = {}
        desc['id'] = iid
        desc['title'] = title
        desc['description'] = description
        fo = format_output.FormatOutput(self.config, [link_path+"/items/"+iid])
        desc['extent'] = extent
        desc['links'] = fo.create_links(False)
        return desc



def to_json(dict_):
    """
    serialize dict to json

    :param dict_: dict_

    :returns: JSON string representation
    """

    return json.dumps(dict_)

def _render_j2_template(config, template, data):
    """
    render Jinja2 template

    :param config: dict of configuration
    :param template: template (relative path)
    :param data: dict of data

    :returns: string of rendered template
    """

    env = Environment(loader=FileSystemLoader(TEMPLATES))
    env.filters['to_json'] = to_json
    env.globals.update(to_json=to_json)

    template = env.get_template(template)
    return template.render(config=config, data=data, version=VERSION)

