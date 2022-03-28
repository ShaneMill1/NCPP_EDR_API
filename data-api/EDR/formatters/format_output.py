from EDR.formatters.jsonconv import json2html
from dicttoxml import dicttoxml
import yaml
import json
import glob
import copy
import EDR.isodatetime.parsers as parse
import EDR.isodatetime.dumpers as dump
from EDR import util
from EDR.provider.metadata import MetadataProvider
from EDR.cache_logic import collection_cache
from siphon.catalog import TDSCatalog
from siphon.http_util import session_manager
from datetime import datetime, timedelta
import os
import pyproj
from EDR.provider.thredds_catalogue import ThreddsCatalogueProvider
from EDR.provider.ndfd_metadata import NDFDMetadataProvider
from flask import request as rq
import sqlite3
import xarray as xr
import fsspec
import pandas as pd
from bs4 import BeautifulSoup
import requests
import s3fs
import numpy as np

class FormatOutput(object):
    def __init__(self, config,link_path, met_ip=False):
        """
        Initialize object

        """
        self.config=config
        self.result = None
        self.server = config['server']['url']
        self.link_path = link_path
        self.datasets = config['datasets']
        self.ndfd_config = None
        self.mp_ = MetadataProvider(config)
        self.met_ip = met_ip

    def get_json(self, input):
        return json.dumps(input)

    def get_yaml(self, input):
        return yaml.dump(input)

    def get_xml(self, input):
        return dicttoxml(input, attr_type=False)

    def get_html(self, input):
        return json2html.convert(input)

    def create_links(self, nest):
        links = []
        if self.result is not None:
            links = self.result

        if nest:
            for p in self.link_path:
                links.append(self.link_template([], p))
        else:
            for p in self.link_path:
                links = self.link_template(copy.deepcopy(links), p)
        self.result = links
        return links

    def link_template(self, links, inpath):
        in_parts = inpath[1:].split('/')
        if 'conformance' in in_parts[-1]:
           links.append({
              "href": self.server + inpath + "?f=application%2Fjson",
              "rel": "conformance",
              "type": "application/json",
              "title": in_parts[-1]  + " document as json"
           })
           #links.append({
           #   "href": self.server + inpath + "?f=application%2Fx-yaml",
           #   "rel": "alternate",
           #   "type": "application/x-yaml",
           #   "title": in_parts[-1] + " document as yaml"
           #})
           #links.append({
           #   "href": self.server + inpath + "?f=text%2Fxml",
           #   "rel": "alternate",
           #   "type": "text/xml",
           #   "title": in_parts[-1] + " document as xml"
           #})
           #links.append({
           #   "href": self.server + inpath + "?f=text%2Fhtml",
           #   "rel": "alternate",
           #   "type": "text/html",
           #   "title": in_parts[-1] + " document as html"
           #})
        elif 'collection' in in_parts[-1]:
           links.append({
              "href": self.server + inpath + "?f=application%2Fjson",
              "rel": "data",
              "type": "application/json",
              "title": in_parts[-1]  + " document as json"
           })
           #links.append({
           #   "href": self.server + inpath + "?f=application%2Fx-yaml",
           #   "rel": "alternate",
           #   "type": "application/x-yaml",
           #   "title": in_parts[-1] + " document as yaml"
           #})
           #links.append({
           #   "href": self.server + inpath + "?f=text%2Fxml",
           #   "rel": "alternate",
           #   "type": "text/xml",
           #   "title": in_parts[-1] + " document as xml"
           #})
           #links.append({
           #   "href": self.server + inpath + "?f=text%2Fhtml",
           #   "rel": "alternate",
           #   "type": "text/html",
           #   "title": in_parts[-1] + " document as html"
           #})
        elif 'api' in in_parts[-1]:
           links.append({
              "href": self.server + inpath + "?f=application%2Fjson",
              "rel": "service",
              "type": "application/json",
              "title": in_parts[-1]  + " document as json"
           })
           #links.append({
           #   "href": self.server + inpath + "?f=application%2Fx-yaml",
           #   "rel": "alternate",
           #   "type": "application/x-yaml",
           #   "title": in_parts[-1] + " document as yaml"
           #})
           #links.append({
           #   "href": self.server + inpath + "?f=text%2Fxml",
           #   "rel": "alternate",
           #   "type": "text/xml",
           #   "title": in_parts[-1] + " document as xml"
           #})
           #links.append({
           #   "href": self.server + inpath + "?f=text%2Fhtml",
           #   "rel": "alternate",
           #   "type": "text/html",
           #   "title": in_parts[-1] + " document as html"
           #})

        else:
           links.append({
              "href": self.server + inpath + "?f=application%2Fjson",
              "rel": "self",
              "type": "application/json",
              "title": in_parts[-1]  + " document as json"
           })
           #links.append({
           #   "href": self.server + inpath + "?f=application%2Fx-yaml",
           #   "rel": "alternate",
           #   "type": "application/x-yaml",
           #   "title": in_parts[-1] + " document as yaml"
           #})
           #links.append({
           #   "href": self.server + inpath + "?f=text%2Fxml",
           #   "rel": "alternate",
           #   "type": "text/xml",
           #   "title": in_parts[-1] + " document as xml"
           #})
           #links.append({
           #   "href": self.server + inpath + "?f=text%2Fhtml",
           #   "rel": "alternate",
           #   "type": "text/html",
           #   "title": in_parts[-1] + " document as html"
           #})

        return links

    def link_template_point(self, links, inpath):
        in_parts = inpath[1:].split('/')
        links.append({
            "href": self.server + inpath + "",
            "rel": "self",
            "type": "point",
            "title": "Point Query",
            "self": "Point"
        })
        return links

    def link_template_radius(self, links, inpath):
        in_parts = inpath[1:].split('/')
        links.append({
            "href": self.server + inpath + "",
            "rel": "self",
            "type": "radius",
            "title": "Radius Query",
            "self": "Radius"
        })
        return links

    def link_template_polygon(self, links, inpath):
        in_parts = inpath[1:].split('/')
        links.append({
            "href": self.server + inpath + "",
            "rel": "self",
            "type": "area",
            "title": "Area Query",
            "self": "Area"
        })
        return links


    def link_template_cube(self, links, inpath):
        in_parts = inpath[1:].split('/')
        links.append({
            "href": self.server + inpath + "",
            "rel": "self",
            "type": "cube",
            "title": "Cube Query",
            "self": "Cube"
        })
        return links


    def link_template_trajectory(self, links, inpath):
        in_parts = inpath[1:].split('/')
        links.append({
            "href": self.server + inpath + "",
            "rel": "self",
            "type": "trajectory",
            "title": "Trajectory Query",
            "self": "Trajectory"
        })
        return links

    def link_template_corridor(self, links, inpath):
        in_parts = inpath[1:].split('/')
        links.append({
            "href": self.server + inpath + "",
            "rel": "self",
            "type": "corridor",
            "title": "Corridor Query",
            "self": "Corridor"
        })
        return links



    def get_parameter_list(self, collname):

        parameters = []
        try:
           for c in collection_cache['data']['collections']:           
               if c['id'] == collname:
                   for p in c['parameters']:
                       parameters.append(p)
        except:
           pass            
        return parameters

    def collections_description(self, collname, display_links):
        global collection_cache
        print(collname)
        if collection_cache == None:
            collection_cache = {}
            collection_cache['update'] = (datetime.now() - timedelta(hours=5))

        if collection_cache['update'] < (datetime.now() - timedelta(minutes=5)):

            collections = self.collection_loop()
            collection_cache = {}
            collection_cache['update'] = datetime.now()
            collections['links'] = self.create_links(False)
            collection_cache['data'] = collections
        output = {}
        collections = self.collection_loop()
        if collname == "all":
            output = self.all_values(output, collections, display_links)
        elif collname.find("summary£") > -1:
            output = self.summary(collname)
        elif collname.find("extent£") > -1:
            output = self.extent(collname, output)
        elif 'automated' in collname:
            output = self.automated(output, display_links)
        elif collname == 'wwa_active':
            output = self.wwa_active_all(output, display_links)
        else:
            output = self.other(collname, output)
        return output


    def items_description(self, collname, display_links):
        output = {}
        global collection_cache
        if collection_cache == None:
            collection_cache = {}
            collection_cache['update'] = (datetime.now() - timedelta(hours=5))

        if collection_cache['update'] < (datetime.now() - timedelta(minutes=5)):

            collections = self.collection_loop()
            collection_cache = {}
            collection_cache['update'] = datetime.now()
            collections['links'] = self.create_links(False)
            collection_cache['data'] = collections
        output = self.items(collname, output)


        return output




    def collection_loop(self):
       
        collections = {}
        collections['collections'] = []        
        for c in self.datasets:
            collection = self.datasets[c]
            if collection["provider"]["data_source"].find("thredds") > -1:
                try: 
                    tc = ThreddsCatalogueProvider()
                    collections['collections'].extend(tc.query_catalogue(self.mp_, self.server, collection))
                except:
                    print ("Thredds server unavailable")
            elif "metar_tgftp" in collection["provider"]["name"]:
                description = self.other_metadata(collection, c)
                description["crs"] = [{"id":"EPSG:4326","wkt":util.proj2wkt(util.WGS84)}]
                description["f"] = self.config['datasets'][collection["provider"]["name"]]['provider']['output_formats']
                collections['collections'].append(description)     
            elif collection['provider']['name']=='wifs_png' or collection['provider']['name']=='national_water_model':
                description = self.other_metadata(collection, c)
                description["f"] = self.config['datasets'][collection["name"]]['provider']['output_formats']
                collections['collections'].append(description)
            elif "rtma_xml" in  collection["provider"]["name"]:
                col_json=self.config['datasets']['rtma_xml']['provider']['data_source']+'/rtma_xml/latest/latest_rtma_collection.json'
                with open(col_json, 'r') as col_json:
                   col=json.load(col_json)
                description = self.other_metadata(collection, c)
                description["crs"] = [{"id":"EPSG:4326","wkt":util.proj2wkt(util.WGS84)}]
                description["polygon-query-options"] = {}
                description["polygon-query-options"]["interpolation-x"] = ["nearest_neighbour"]
                description["polygon-query-options"]["interpolation-y"] = ["nearest_neighbour"]
                description["point-query-options"] = {}
                description["point-query-options"]["interpolation"] = ["nearest_neighbour"]
                description["f"] = self.config['datasets'][collection["provider"]["name"]]['provider']['output_formats']
                description['parameters']={}
                for p in col[0]['parameters']:
                   description['parameters'][p]={}
                   description['parameters'][p]['description']={'en':p}
                   description['parameters'][p]['observed-property']={}
                   description['parameters'][p]['observed-property']['label']={'en':p}
                   description['parameters'][p]['extent']={}
                   description['parameters'][p]['extent']['temporal']={}
                   description['parameters'][p]['extent']['temporal']['name']=[p]
                   description['parameters'][p]['extent']['temporal']['coordinates']=[p]
                   description['parameters'][p]['extent']['temporal']['range']=col[0]['forecast_time']
                collections['collections'].append(description)
            else:
                description = self.other_metadata(collection, c)
                description["crs"] = [{"id":"EPSG:4326","wkt":util.proj2wkt(util.WGS84)}]
                description["polygon-query-options"] = {}
                description["polygon-query-options"]["interpolation-x"] = ["nearest_neighbour"]
                description["polygon-query-options"]["interpolation-y"] = ["nearest_neighbour"]
                description["point-query-options"] = {}
                description["point-query-options"]["interpolation"] = ["nearest_neighbour"]
                description["f"] = collection['provider']['output_formats']
                collections['collections'].append(description)
        return collections


    def get_instance_value(self, collection):
        try:
            collection['id'].split('_')
            collection['instance-axes'] = copy.deepcopy(self.datasets[collection['id'].split('_')[0]]['instance-axes'])
            if ('z' in collection['instance-axes']) and ('vertical' in collection['extent']):
                if len(collection['extent']['vertical']['name'][0]) > 0:
                    lower_bound, upper_bound = self.get_height_min_max(collection['extent']['vertical']['range'])
                    collection['instance-axes']['z']['lower-bound'] = lower_bound
                    collection['instance-axes']['z']['upper-bound'] = upper_bound
                    if 'units' in collection['extent']['vertical']:
                        collection['instance-axes']['z']['label'] = collection['extent']['vertical']['units']
                    if 'unit_desc' in collection['extent']['vertical']:
                        collection['instance-axes']['z']['uom-label'] = collection['extent']['vertical']['unit_desc']
                else:
                    del collection['instance-axes']['z']
            if ('t' in collection['instance-axes']) and ('temporal' in collection['extent']):
                lower_bound, upper_bound = self.get_time_min_max(collection['extent']['temporal']['range'])
                
                collection['instance-axes']['t']['lower-bound'] = lower_bound
                collection['instance-axes']['t']['upper-bound'] = upper_bound
            del collection['extent']
        except:
            print('Invalid collection')

        return collection

    def get_item_value(self,collection):
       return collection


    def get_time_min_max(self, iso_times):
        try:
           datetime_list = []
           for itime in iso_times:
              datetime_list.append(parse.TimePointParser().parse(itime))
           lower_bound = dump.TimePointDumper().dump(min(datetime_list),'CCYY-MM-DDThh:mm:ssZ')
           upper_bound = dump.TimePointDumper().dump(max(datetime_list),'CCYY-MM-DDThh:mm:ssZ')
        except:
           lower_bound=''
           upper_bound=''
        return lower_bound, upper_bound

    def get_height_min_max(self, heights):

        height_list = []
        for height in heights:
            height_list.append(float(height))
        lower_bound = min(height_list)
        upper_bound = max(height_list)

        return lower_bound, upper_bound   

    def all_values(self, output, collections, display_links):
        output['collections'] = []
        for c in collection_cache['data']['collections']:
            collection_detail = {}
            collection_detail['id'] = c['id']
            collection_detail['title'] = c['title']
            collection_detail['description'] = c['description']
            collection_detail['extent'] = {}
            try:
               collection_detail['extent'] = util.geographictoextent(c['extent']['horizontal']['geographic'])
            except:
               pass
            url_parts = c['links'][0]['href'].split('/')
            for idx,url_p in enumerate(url_parts):
               if 'automated' in url_p:
                  new=url_p.split('_')[0]+'_'+url_p.split('_')[1]
                  url_parts[idx]='collections/'+new
               if 'ndfd_xml' in url_p:
                  url_parts[idx]='collections/ndfd_xml'
               if 'rtma_xml' in url_p:
                  url_parts[idx]='collections/rtma_xml'
               #this is a little clunky, need to make this a little better - SM 6/2/21
               if 'goes' in url_p or 'himawari' in url_p:
                  url_parts[idx]='collections/'+url_p
            #if c['id']=='wwa_active':
            #   collection_detail['links']=c['links']
            else:
               collection_detail['links'] = self.link_template( [], "/" + url_parts[-4] + "/" + url_parts[-3])
            output['collections'].append(collection_detail)
        if display_links:
            output['links'] =  self.link_template( [], "/collections")
        return output
     
    def automated(self, output, display_links):
        collection_detail = {}
        col_ids=[]
        link_list=[]
        fs=s3fs.S3FileSystem()
        data_location=self.config['datasets']['automated_gfs']['provider']['data_source']
        cycles=fs.glob(data_location+'/gfs_100/*')
        for c in sorted(cycles):
           z_objects=fs.glob('s3://'+c+'/*')
           c_short=os.path.basename(c)
           for z in z_objects:
              z_short=os.path.basename(z)
              col_ids.append(z_short)
              link_list.append("/collections/automated_"+z_short+"/instances/"+c_short)
        link_template_list=[]
        for l in link_list:
            link_template_list.append(self.link_template( [], l))
        output['links']= self.link_template( [], rq.path)
        output['members']=link_template_list
        return output
   
    def wwa_active_all(self, output, display_links):
        collection_detail = {}
        col_ids=[]
        output['id']='wwa_active'
        source_url=self.config['datasets']['wwa_active']['provider']['data_source']
        wwa_home=requests.get(source_url)
        soup=BeautifulSoup(wwa_home.text,'html.parser')
        cap_events=soup.find_all('cap:event')
        wwa_home.close()
        col_link_list=list()
        col_name_list=list()
        for cp in cap_events:
           event_name='wwa_active_'+cp.string.replace(' ','_').lower()
           if event_name not in col_name_list:
              col_name_list.append(event_name)
              col_link_list.append('/collections/'+event_name+'')
        link_template_list=[]
        for l in col_link_list:
            link_template_list.append(self.link_template( [], l))
        output['members']=link_template_list
        return output

    def automated_collection_desc(self, collname, display_links):
        output={}
        auto_cycle=rq.url.split('/')[-1].split('?')[0]
        cid="_".join(collname.split("_", 2)[1:])
        point='/collections/'+collname+'/instances/'+auto_cycle+'/position'
        point_query_selector=point+'_query_selector'
        polygon='/collections/'+collname+'/instances/'+auto_cycle+'/area'
        polygon_query_selector=polygon+'_query_selector'
        radius='/collections/'+collname+'/instances/'+auto_cycle+'/radius'
        radius_query_selector='/collections/'+collname+'/instances/'+auto_cycle+'/radius_query_selector'
        cube='/collections/'+collname+'/instances/'+auto_cycle+'/cube'
        cube_query_selector=cube+'_query_selector'
        trajectory='/collections/'+collname+'/instances/'+auto_cycle+'/trajectory'
        trajectory_query_selector=trajectory+'_query_selector'
        corridor='/collections/'+collname+'/instances/'+auto_cycle+'/corridor'
        corridor_query_selector=corridor+'_query_selector'
        orig=self.link_template( [], '/collections/'+collname+'/instances/'+auto_cycle)
        point=self.link_template_point( [], point)
        radius=self.link_template_radius([],radius)
        polygon=self.link_template_polygon( [], polygon)
        cube=self.link_template_cube( [], cube)
        trajectory=self.link_template_trajectory( [], trajectory)
        corridor=self.link_template_corridor( [], corridor)
        point_query_selector=self.link_template_point( [], point_query_selector)
        radius_query_selector=self.link_template_radius( [], radius_query_selector)
        polygon_query_selector=self.link_template_polygon( [], polygon_query_selector)
        cube_query_selector=self.link_template_cube( [], cube_query_selector) 
        trajectory_query_selector=self.link_template_trajectory( [], trajectory_query_selector)
        corridor_query_selector=self.link_template_corridor( [], corridor_query_selector)
        link_list=orig+point+radius+polygon+cube+trajectory+corridor+point_query_selector+radius_query_selector+polygon_query_selector+cube_query_selector+trajectory_query_selector+corridor_query_selector
        output['links']=link_list
        output['collections']=collname
        output['title']=collname
        output['parameters']={}
        #open zarr here
        #collname is collection name
        #data_location is 's3://enviroapi-bucket-1/zarr'
        fs=s3fs.S3FileSystem()
        data_location=self.config['datasets']['automated_gfs']['provider']['data_source']
        instance=rq.url.split('/')[-1].split('?')[0]
        zarr_object_name=rq.url.split('/')[-3].replace('automated_','')
        zarr_open=data_location+'/gfs_100/'+instance+'/'+zarr_object_name
        collection_ds=xr.open_zarr(zarr_open)
        for idx,p in enumerate(collection_ds.data_vars):
           if p!='time':
              for co in collection_ds.coords:
                 if '_time' in co:
                    f_key=co
              #determine if pynio terms or cfgrib terms
              if hasattr(collection_ds[p],'GRIB_shortName'):
                 meta='cfgrib'
              else:
                 meta='pynio'
              if meta=='cfgrib':
                 short_name=collection_ds[p].GRIB_shortName
                 type_of_level=collection_ds[p].GRIB_typeOfLevel
              if meta=='pynio':
                 short_name=p.split('_')[0]
                 type_of_level=collection_ds[p].level_type   
              if not f_key:
                 output['parameters'].update({p: {\
                 'description': {'en': short_name},
                 'unit': {'label':{'en': ''} ,'symbol':{'value':'','type':''}},
                 'observed-property': {'label': {'en':  collection_ds[p].units}},
                 'extent': {'horizontal': {'name': ['longitude','latitude'],'coordinates': ['x','y'],'geographic': ""}}}})
              if f_key:
                 grib_name=short_name
                 time_values=collection_ds[f_key].values.astype(str).tolist()
                 if isinstance(time_values,str):
                    time_values=[time_values]
                 output['parameters'].update({p: {\
                 'description': {'en': short_name},
                 'unit': {'label':{'en': ''} ,'symbol':{'value':'','type':''}},
                 'observed-property': {'label': {'en':  collection_ds[p].units}}, 
                 'extent': {'temporal': {'name': ['time'],'coordinates':['time'], 'range': time_values},
                 'horizontal': {'name': ['longitude','latitude'],'coordinates': ['x','y'],'geographic': ""}}}})
                 dims='\t'.join(collection_ds.keys())      
                 for l in collection_ds.coords:
                    if meta=='cfgrib':
                       if l==collection_ds[p].GRIB_typeOfLevel:
                          level=l
                          c_list=list()
                          c_list=collection_ds[l].values.astype(str).tolist() 
                          try:
                             output['parameters'].update({p: {\
                             'description': {'en': collection_ds[p].GRIB_shortName},
                             'unit': {'label':{'en': ''} ,'symbol':{'value':'','type':''}},
                             'observed-property': {'label': {'en':  collection_ds[p].GRIB_shortName}},
                             'extent': {'temporal': {'name': ['time'],'coordinates':['time'], 'range': time_values},'horizontal':\
                             {'name': ['longitude','latitude'],\
                             'coordinates': ['x','y'],'geographic': "BBOX[]"},'vertical':{'name':[level],'coordinates':['z'],'range':c_list}}}})
                          except:
                             output['parameters'].update({p: {\
                             'description': {'en': collection_ds[p].GRIB_shortName},
                             'unit': {'label':{'en': ''} ,'symbol':{'value':'','type':''}},
                             'observed-property': {'label': {'en':  collection_ds[p].GRIB_shortName}},
                             'extent': {'temporal': {'name': ['time'],'coordinates':['time'], 'range': time_values},'horizontal':\
                             {'name': ['longitude','latitude'],\
                             'coordinates': ['x','y'],'geographic': "BBOX[]"},'vertical':{'name':[level],'coordinates':['z'],'range':c_list}}}})
                    if meta=='pynio':
                       if 'lv_' in l:
                          level=l
                          c_list=list()
                          c_list=collection_ds[l].values.astype(str).tolist()
                          try:
                             output['parameters'].update({p: {\
                             'description': {'en': short_name},
                             'unit': {'label':{'en': ''} ,'symbol':{'value':'','type':''}},
                             'observed-property': {'label': {'en':  short_name}},
                             'extent': {'temporal': {'name': ['time'],'coordinates':['time'], 'range': time_values},'horizontal':\
                             {'name': ['longitude','latitude'],\
                             'coordinates': ['x','y'],'geographic': "BBOX[]"},'vertical':{'name':[level],'coordinates':['z'],'range':c_list}}}})
                          except:
                             output['parameters'].update({p: {\
                             'description': {'en': short_name},
                             'unit': {'label':{'en': ''} ,'symbol':{'value':'','type':''}},
                             'observed-property': {'label': {'en':  short_name}},
                             'extent': {'temporal': {'name': ['time'],'coordinates':['time'], 'range': time_values},'horizontal':\
                             {'name': ['longitude','latitude'],\
                             'coordinates': ['x','y'],'geographic': "BBOX[]"},'vertical':{'name':[level],'coordinates':['z'],'range':c_list}}}})

              provider=collname.split('_')[0]+'_'+collname.split('_')[1]
              output['f']=self.config['datasets'][provider]['provider']['output_formats']
              output['crs']=[{"id":"EPSG:4326","wkt":util.proj2wkt(util.WGS84)}]
              output['point-query-options']={}
              output['point-query-options']['interpolation']=['nearest_neighbor']
              output['polygon-query-options']={}
              output['polygon-query-options']['interpolation-x']=['nearest_neighbor']
              output['polygon-query-options']['interpolation-y']=['nearest_neighbor']
              output['id']=collname
              output['instance-axes']={}
              if meta=='cfgrib':
                 output['instance-axes']['x']={'label': 'Longitude', 'lower-bound': collection_ds[p].GRIB_longitudeOfFirstGridPointInDegrees,\
                 'upper-bound': collection_ds[p].GRIB_longitudeOfLastGridPointInDegrees, 'uom-label': "degrees"}
                 output['instance-axes']['y']={'label': 'Latitude', 'lower-bound': collection_ds[p].GRIB_latitudeOfFirstGridPointInDegrees,\
                 'upper-bound': collection_ds[p].GRIB_latitudeOfLastGridPointInDegrees, 'uom-label': "degrees"}       
                 level_values=collection_ds[p][collection_ds[p].GRIB_typeOfLevel].values.astype(str).tolist()
                 level=collection_ds[collection_ds[p].GRIB_typeOfLevel]
                 level_units=level.units
                 if len(level_values)>1:
                    output['instance-axes']['z']={'label': collection_ds[p].GRIB_typeOfLevel, 'lowerBound': level_values[0], 'upper-bound': level_values[-1], 'uom-label': level_units}
                 else:
                    output['instance-axes']['z']={'label': collection_ds[p].GRIB_typeOfLevel, 'lowerBound': level_values[0], 'upper-bound': level_values[0], 'uom-label': level_units}
              if meta=='pynio':
                 output['instance-axes']['x']={'label': 'Longitude', 'lower-bound': collection_ds[p].lon_0.values.tolist()[0],\
                 'upper-bound': collection_ds[p].lon_0.values.tolist()[-1], 'uom-label': "degrees"}
                 output['instance-axes']['y']={'label': 'Latitude', 'lower-bound': collection_ds[p].lat_0.values.tolist()[0],\
                 'upper-bound': collection_ds[p].lat_0.values.tolist()[-1], 'uom-label': "degrees"}
                 for cl in collection_ds[p].coords:
                    if 'lv_' in cl:
                       level=cl
                       level_values=collection_ds[p][level].values.astype(str).tolist()
                       level_units=collection_ds[p][level].units
                       if len(level_values)>1:
                          output['instance-axes']['z']={'label': type_of_level, 'lowerBound': level_values[0], 'upper-bound': level_values[-1], 'uom-label': level_units}
                       else:
                          output['instance-axes']['z']={'label': type_of_level, 'lowerBound': level_values[0], 'upper-bound': level_values[0], 'uom-label': level_units}
              valid_times=collection_ds[f_key].values.astype(str)
              valid_times=time_values
              output['instance-axes']['t']={'label': 'Time', 'lower-bound': valid_times[0], 'upper-bound': valid_times[-1], 'uom-label': "ISO8601"}
        return output


    def find_initial_time(self,initial_time):
       initial_time=initial_time.replace('(','')
       initial_time=initial_time.replace(')','')
       initial_time=initial_time.replace('/','-')
       month=initial_time[0:2]
       day=initial_time[3:5]
       year=initial_time[6:10]
       time=initial_time[11:16]
       initial_time=year+'-'+month+'-'+day+' '+time+':00'
       initial_time=datetime.strptime(initial_time, "%Y-%m-%d %H:%M:%S")
       return initial_time

    def summary(self, collname):
        output = []
        self.link_path = []
        scol = collname.split("£")[1]
        for c in collection_cache['data']['collections']:
            if c['id'].find(scol) > -1:
                self.link_path.append('/collections/'+c['id'])          
        output = self.create_links(True)

        return output    
    
    def extent(self, collname, output):
        try:
           scol = collname.split("£")[1]
           for c in collection_cache['data']['collections']:           
              if c['id'] == scol:
                  output = {}
                  output['spatial'] = c['extent']['horizontal']['geographic']
                  if 'temporal' in c['extent']:
                      lower_bound, upper_bound = self.get_time_min_max(c['extent']['temporal']['range'])
                      output['temporal'] = lower_bound + "/" + upper_bound
                  if ('vertical' in c['extent']) and (len(c['extent']['vertical']['name'][0]) > 0):
                      lower_bound, upper_bound = self.get_height_min_max(c['extent']['vertical']['range'])
                      output['vertical'] = str(lower_bound) + "/" + str(upper_bound)
        except:
           output=output
        return output
    
    def other(self, collname, output):
        collection_detail = {}
        for c in collection_cache['data']['collections']:
            
            if c['id'] == collname:
                collection_detail =  c
        collection_detail = self.get_instance_value(copy.deepcopy(collection_detail))
        if 'links' in collection_detail:
            query_links = []
            for val in collection_detail:
                if val == 'links':
                    query_links.extend(collection_detail[val])
                output[val] = collection_detail[val]
            url_parts = query_links[0]['href'].split('/')
            output['links'] = self.link_template( [], "/" + url_parts[-4] + "/" + url_parts[-3] + "/" + url_parts[-2])
            for qlink in query_links:
                if qlink['href'].lower().find('position') > -1:
                    qlink['self'] = "Point"
                    qlink['title'] = "Position query"
                if qlink['href'].lower().find('area') > -1:
                    qlink['self'] = "Polygon"
                    qlink['title'] = "Area query"
                if qlink['href'].lower().find('cube') > -1:
                    qlink['self'] = "Cube"
                    qlink['title'] = "Cube query"
                if qlink['href'].lower().find('trajectory') > -1:
                    qlink['self'] = "Trajectory"
                    qlink['title'] = "Trajectory query"
                if qlink['href'].lower().find('corridor') > -1:
                    qlink['self'] = "Corridor"
                    qlink['title'] = "Corridor query"
                output['links'].append(qlink)
        return output

    def wifs_metar_items(self,collname):
        collection_detail = {}; link_dict={}
        output_formats=self.config['datasets'][collname]['provider']['output_formats']
        for c in collection_cache['data']['collections']:
            if c['id'] == collname:
                collection_detail =  c
        output=collection_detail
        output['links'][0]['href']=self.server+'/'+rq.path.replace('items','locations')
        rel_=output['links'][0]['rel']
        type_=output['links'][0]['type']
        title_=output['links'][0]['title']
        base_link=self.server+rq.path.replace('items','locations')
        base_link=base_link.split('?')
        output['links']=[]
        for o in output_formats:
           link_dict={}
           if 'collective' in rq.url or 'decoded' in rq.url:
              if o == 'ascii':
                 link_dict['href']=base_link[0]+'?f=application/'+o
                 link_dict['rel']=rel_
                 link_dict['type']=type_
                 link_dict['title']=title_
                 output['links'].append(link_dict)
           else:
              if o == 'json':
                 link_dict['href']=base_link[0]+'?f=application/'+o
                 link_dict['rel']=rel_
                 link_dict['type']=type_
                 link_dict['title']=title_
                 output['links'].append(link_dict)
              if o == 'ascii':
                 link_dict['href']=base_link[0]+'?f=text/'+o
                 link_dict['rel']=rel_
                 link_dict['type']=type_
                 link_dict['title']=title_
                 output['links'].append(link_dict)
              if o == 'xml':
                 link_dict['href']=base_link[0]+'?f=application/'+o
                 link_dict['rel']=rel_
                 link_dict['type']=type_
                 link_dict['title']=title_
                 output['links'].append(link_dict)
              if o == 'png':
                 link_dict['href']=base_link[0]+'?f=image/'+o
                 link_dict['rel']=rel_
                 link_dict['type']=type_
                 link_dict['title']=title_
                 output['links'].append(link_dict)
        #Tie the extent to a geographic point
        try:
           output['extent']['horizontal']['geographic']=output['extent']['horizontal']['geographic'].replace('BBOX[placeholder]','')
           #Access sqlite3 database
           db_file='EDR/stations.sqlite'
           station=base_link[0].split('_')[1].split('/')[2]
           conn = None
           conn = sqlite3.connect(db_file)
           cur = conn.cursor()
           cur.execute("select lat,long from stations where icao=='"+str(station)+"';")
           rows = cur.fetchall()
           conn.close()
           lat_lon=rows[0]
           lat=lat_lon[0]
           lon=lat_lon[1]
           if 'N' in lat:
              lat_clean=lat.split(' ')[0].lstrip('0')+'.'+lat.split(' ')[1].replace('N','')
           if 'S' in lat:
              lat_clean='-'+lat.split(' ')[0].lstrip('0')+'.'+lat.split(' ')[1].replace('S','')
           if 'E' in lon:
              lon_clean=lon.split(' ')[0].lstrip('0')+'.'+lon.split(' ')[1].replace('E','')
           if 'W' in lon:
              lon_clean='-'+lon.split(' ')[0].lstrip('0')+'.'+lon.split(' ')[1].replace('W','')
           ll_wkt='POINT('+lon_clean+', '+lat_clean+')'
           output['extent']['horizontal']['geographic']=ll_wkt
        except:
           print('collective will use default wkt')
        return output

    def wwa_active_items(self,collname):
        collection_detail = {}; link_dict={}
        coll='wwa_active'
        output_formats=self.config['datasets'][coll]['provider']['output_formats']
        for c in collection_cache['data']['collections']:
            if c['id'] == coll:
                collection_detail =  c
        output=collection_detail
        output['links'][0]['href']=self.server+'/'+rq.path.replace('items','locations')
        rel_=output['links'][0]['rel']
        type_=output['links'][0]['type']
        title_=output['links'][0]['title']
        base_link=self.server+rq.path.replace('items','locations')
        base_link=base_link.split('?')
        output['links']=[]
        for o in output_formats:
           link_dict={}
           if o == 'cap':
              link_dict['href']=base_link[0]+'?f=text/'+o
              link_dict['rel']=rel_
              link_dict['type']=type_
              link_dict['title']=title_
              output['links'].append(link_dict)
           if o == 'json':
              link_dict['href']=base_link[0]+'?f=application/'+o
              link_dict['rel']=rel_
              link_dict['type']=type_
              link_dict['title']=title_
              output['links'].append(link_dict)
        return output


    def items(self, collname, output):
        collection_detail = {}; link_dict={}
        if collname=='wifs_png' or collname == 'metar_tgftp':
           output=self.wifs_metar_items(collname)
        if 'wwa_active' in collname:
           output=self.wwa_active_items(collname)
        return output


    def other_metadata(self, collection, c_id):
        if c_id == "metar":
            description = self.get_metar_metadata(collection, c_id)
        elif c_id == "osmhighways" or c_id == "dem":
            description = self.get_simple_metadata(collection, c_id)
        elif c_id == "taf":
            description = self.get_taf_metadata(collection, c_id)
        elif c_id == "tca":
            description = self.get_tca_metadata(collection, c_id)
        elif c_id == "metar_tgftp":
            description = self.get_metartgftp_metadata(collection, c_id)
        elif c_id == "vaa":
            description = self.get_vaa_metadata(collection, c_id)
        elif "automated" in c_id:
            description = self.get_automated_metadata(collection, c_id)
        elif "ndfd_xml" in c_id:
            description = self.get_ndfdxml_metadata(collection, c_id)
        elif "rtma_xml" in c_id:
            description = self.get_rtmaxml_metadata(collection, c_id)
        elif 'himawari' in c_id:
            description = self.get_himawari_metadata(collection, c_id)
        elif 'goes' in c_id:
            description = self.get_goes_metadata(collection, c_id)
        elif c_id == 'wwa_active':
            description = self.get_wwa_active_metadata(collection, c_id)
        elif c_id == 'wifs_png':
            description = self.get_wifspng_metadata(collection, c_id)
        elif c_id == 'national_water_model':
            description = self.get_nwm_metadata(collection, c_id)
        return description


    def get_taf_metadata(self, collection, c_id):
        temporal = []
        tperiod = timedelta(hours=-6)
        now = datetime.strptime(datetime.now().strftime(
            "%d-%m-%YT%H:00"), "%d-%m-%YT%H:%M")
        now = now + tperiod
        for tloop in range(0, 6, 1):
            temporal.append(
                (now + timedelta(hours=tloop)).isoformat()+'Z')
        link_str1 = '{0}/collections/{1}/raw/position'.format(self.server, c_id)
        link_str2 = 'Point query for raw {0}'.format(collection['title'])
        description = {"id": c_id,
                       "title": self.datasets[c_id]['title'],
                       "description": collection['description'],
                        "extent": {
                            "horizontal": util.horizontaldef(["longitude","latitude"], ["x","y"], collection['extent']['spatial'].split(',')),
                            "temporal": util.coorddef(["time"],["time"], temporal)
                        },
                       "links": [util.createquerylinks(link_str1, 'self', 'position', link_str2)]}
        description['parameters']={}
        description['parameters']['all']={}
        description['parameters']['all']['extent']=description['extent']
        return description

    def get_tca_metadata(self, collection, c_id):
        url_t=rq.url
        url_t=url_t.split('/')
        temporal='' 
        item_id=rq.url.split('/')[-1]
        link_str1 = self.server+'/collections/'+c_id+'/locations/'+item_id
        link_str2 = 'Location query for '+ c_id
        description = {"id": c_id,
                       "title": self.datasets[c_id]['title'],
                       "description": collection['description'],
                        "extent": {
                            "horizontal": util.horizontaldef(["longitude","latitude"], ["x","y"], collection['extent']['spatial'].split(',')),
                            "temporal": util.coorddef(["time"],["time"], temporal)
                        },
                       "links": [util.createquerylinks(link_str1, 'self', 'locations', link_str2)]}
        description['parameters']={}
        #description['parameters']['all']={}
        #description['parameters']['all']['extent']=description['extent']
        return description


    def get_metartgftp_metadata(self, collection, c_id):
        url_t=rq.url
        url_t=url_t.split('/')
        temporal=''
        item_id=rq.url.split('/')[-1]
        link_str1 = self.server+'/collections/'+c_id+'/locations/'+item_id
        link_str2 = 'Location query for '+ c_id
        description = {"id": c_id,
                       "title": self.datasets[c_id]['title'],
                       "description": collection['description'],
                        "extent": {
                            "horizontal": util.horizontaldef(["longitude","latitude"], ["x","y"], collection['extent']['spatial'].split(',')),
                            "temporal": util.coorddef(["time"],["time"], temporal)
                        },
                       "links": [util.createquerylinks(link_str1, 'self', 'locations', link_str2)]}
        description['parameters']={}
        #description['parameters']['all']={}
        #description['parameters']['all']['extent']=description['extent']
        return description


    def get_wifspng_metadata(self, collection, c_id):
        url_t=rq.url
        url_t=url_t.split('/')
        temporal=''
        item_id=rq.url.split('/')[-1]
        link_str1 = self.server+'/collections/'+c_id+'/locations/'+item_id
        link_str2 = 'Location query for '+ c_id
        description = {"id": c_id,
                       "title": self.datasets[c_id]['title'],
                       "description": collection['description'],
                       "links": [util.createquerylinks(link_str1, 'self', 'locations', link_str2)]}
        description['parameters']={}
        return description

    def get_wwa_active_metadata(self, collection, c_id):
        col_link_list=list(); col_name_list=list()
        item_id=rq.url.split('/')[-1]
        link_str1 = self.server+'/collections/'+c_id+'/locations/'+item_id
        link_str2 = 'Location query for '+ c_id
        description = {"id": c_id,
                       "title": self.datasets[c_id]['title'],
                       "description": collection['description'],
                       "links": [util.createquerylinks(link_str1, 'self', 'locations', link_str2)]}
        description['parameters']={}
        return description


    def get_vaa_metadata(self, collection, c_id):
        url_v=rq.url
        url_v=url_v.split('/')
        instance='missing'
        temporal=''
        link_str1 = self.server+'/collections/'+c_id+'/'+instance+'/position'
        link_str2 = 'Point query for '+instance
        description = {"id": c_id,
                       "title": self.datasets[c_id]['title'],
                       "description": collection['description'],
                        "extent": {
                            "horizontal": util.horizontaldef(["longitude","latitude"], ["x","y"], collection['extent']['spatial'].split(',')),
                            "temporal": util.coorddef(["time"],["time"], temporal)
                        },
                       "links": [util.createquerylinks(link_str1, 'self', 'position', link_str2)]}
        description['parameters']={}
        description['parameters']['all']={}
        description['parameters']['all']['extent']=description['extent']
        return description
    
    def get_metar_metadata(self, collection, c_id):
        temporal = []
        tperiod = timedelta(hours=-36)
        now = datetime.strptime(datetime.now().strftime(
            "%d-%m-%YT%H:00"), "%d-%m-%YT%H:%M")
        now = now + tperiod
        for tloop in range(1, 36, 1):
            temporal.append(
                (now + timedelta(hours=tloop)).isoformat()+'Z')

        description = {"id": c_id,
                    "title": self.datasets[c_id]['title'],
                    "description": collection['description'],
                    "extent": {
                        "horizontal": util.horizontaldef(["longitude","latitude"],["x","y"],collection['extent']['spatial'].split(',')),
                        "temporal": util.coorddef(["time"],["time"],temporal)
                    },
                    "links": [util.createquerylinks(self.server + '/collections/' + c_id + '/raw/position','self','position','Point query for raw ' + collection['title'])]

                    }
        description['parameters'] = {}
        for p in self.datasets[c_id]['parameters']:
            if type(self.datasets[c_id]['parameters'][p][0]) is str:
                parts = self.datasets[c_id]['parameters'][p][0].split(
                    '/')
                if parts[-3] == 'bufr4':
                    description['parameters'][p] = (self.mp_.get_buf4_detail(
                        parts[-3], parts[-2], parts[-1], None))
                elif parts[4] == 'grib2':
                    description['parameters'][p] =(self.mp_.get_grib_detail(
                        parts[-3], parts[-2], parts[-1], None))
                description['parameters'][p]['extent'] = description['extent']
            else:
                description['parameters'][self.datasets[c_id]['parameters'][p][0]['description']['en'].lower().replace(' ','_')] = (self.datasets[c_id]['parameters'][p][0])
                description['parameters'][self.datasets[c_id]['parameters'][p][0]['description']['en'].lower().replace(' ','_')]['extent'] = description['extent']
        return description
    
    def get_simple_metadata(self, collection, c_id):
        description = {"id": c_id,
                    "title": self.datasets[c_id]['title'],
                    "description": collection['description'],
                    "extent": {
                        "horizontal": util.horizontaldef(["longitude","latitude"],["x","y"],collection['extent']['spatial'].split(',')),
                    },
                    "links": [util.createquerylinks(self.server + '/collections/' + c_id + '/latest/position','self','position','Point query for latest ' + collection['title']),util.createquerylinks(self.server + '/collections/' + c_id + '/latest/position_query_selector','self','position','Point query for latest ' + collection['title']).util.createquerylinks(self.server + '/collections/' + c_id + '/latest/area','self','area','Area query for latest ' + collection['title']),util.createquerylinks(self.server + '/collections/' + c_id + '/latest/position','self','area_query_selector','Area query for latest ' + collection['title'])]

                    }
        description['parameters'] = {}
        try:
           for p in self.datasets[c_id]['parameters']:
              description['parameters'][self.datasets[c_id]['parameters'][p][0]['description']['en'].lower().replace(' ','_')] = (self.datasets[c_id]['parameters'][p][0])
              description['parameters'][self.datasets[c_id]['parameters'][p][0]['description']['en'].lower().replace(' ','_')]['extent'] = description['extent']
        except:
           param_list=list()
           for p in self.datasets[c_id]['parameters']:
              param_list.append(p)
           description['parameters'] = param_list
        return description


    def get_nwm_metadata(self, collection, c_id):
        description = {"id": c_id,
                    "title": self.datasets[c_id]['title'],
                    "description": collection['description'],
                    "extent": {
                        "horizontal": util.horizontaldef(["longitude","latitude"],["x","y"],collection['extent']['spatial'].split(',')),
                    },
                    "links": [util.createquerylinks(self.server + '/collections/' + c_id + '/latest/position','self','position','Position query for latest ' + collection['title']),util.createquerylinks(self.server + '/collections/' + c_id + '/latest/position_query_selector','self','position_query_selector','Position query UI for latest ' + collection['title']),util.createquerylinks(self.server + '/collections/' + c_id + '/latest/area','self','area','Area query for latest ' + collection['title']),util.createquerylinks(self.server + '/collections/' + c_id + '/latest/area_query_selector','self','area_query_selector','Area query UI for latest ' + collection['title'])]

                    }
        description['parameters'] = {}
        param_dict={}
        time_csv=self.config['datasets']['national_water_model']['provider']['meta']
        data_var_list=['streamflow','velocity']
        df=pd.read_csv(time_csv, sep=',',header=None)
        times=df.values.tolist()
        time_list=list()
        for t in times:
           time_list.append(t[0])
        for p in data_var_list:
           param_dict[p]={}
           param_dict[p]['extent']={}
           param_dict[p]['extent']['temporal']={}
           param_dict[p]['extent']['temporal']['range']=time_list[-5000:]
        description['parameters'] = param_dict
        return description



    def get_automated_metadata(self, collection, cid):
        model=collection['provider']['model'][0]
        cycle=rq.url.split('/')[-1].split('?')[0]
        description = {"id": cid,
                    "title": self.datasets[cid]['title'],
                    "description": collection['description'],
                    "extent": {
                        "horizontal": util.horizontaldef(["longitude","latitude"],["x","y"],collection['extent']['spatial'].split(',')),
                    },
"links": [util.createquerylinks(self.server + '/collections/automated_'+model + '_' + cid+'/instances/'+cycle+'/position','self','position','Point query for ' + collection['title']),util.createquerylinks(self.server + '/collections/' + cid + '/latest/position_query_selector','self','position_query_selector','Point query for latest ' + collection['title'])]}
        description['parameters'] = {}
        return description
 
    def get_himawari_metadata(self, collection, cid):
        data_source=self.config['datasets'][cid]['provider']['data_source']
        name=self.config['datasets'][cid]['name']
        meta_loc=data_source+'/zarr_collection.json'
        with open(meta_loc, 'r') as col_json:
           col=json.load(col_json)
           time=col[0]['time']
        description = {"id": cid,
                    "title": self.datasets[cid]['title'],
                    "description": collection['description'],
                    "extent": {
                        "horizontal": util.horizontaldef(["longitude","latutude"],["x","y"],collection['extent']['spatial'].split(',')),
                    "temporal": {'range': col[0]['time']},
                    },
"links": [util.createquerylinks(self.server + '/collections/'+cid+'/instances/latest/position','self','position','Point query for ' + collection['title']),util.createquerylinks(self.server + '/collections/' + c_id + '/latest/position_query_selector','self','position_query_selector','Point query for latest ' + collection['title']),util.createquerylinks(self.server + '/collections/'+cid+'/instances/latest/area','self','area','Area query for ' + collection['title']),util.createquerylinks(self.server + '/collections/'+cid+'/instances/latest/area_query_selector','self','area_query_selector','Area query for ' + collection['title']),util.createquerylinks(self.server + '/collections/'+cid+'/instances/latest/cube','self','cube','Cube query for ' + collection['title']),util.createquerylinks(self.server + '/collections/'+cid+'/instances/latest/cube_query_selector','self','cube_query_selector','Cube query for ' + collection['title'])]}
        description['parameters'] = {}
        for params in col[0]['parameters']:
            description['parameters'][params]={}
            description['parameters'][params]['extent']={}
            description['parameters'][params]['extent']['horizontal']={}
            description['parameters'][params]['extent']['temporal']={'range': col[0]['time']}
        return description

    def get_goes_metadata(self, collection, cid):
        name=collection['description']
        data_source=self.config['datasets'][cid]['provider']['data_source']
        name=self.config['datasets'][cid]['name']
        meta_loc=data_source+'/zarr_collection.json'
        with open(meta_loc, 'r') as col_json:
           col=json.load(col_json)
           time=col[0]['time']
        description = {"id": name,
                    "title": name,
                    "description": collection['description'],
                    "extent": {
                        "horizontal": util.horizontaldef(["longitude","latutude"],["x","y"],collection['extent']['spatial'].split(',')),
                    "temporal": {'range': col[0]['time']},
                    },
"links": [util.createquerylinks(self.server + '/collections/'+cid+'/instances/latest/position','self','position','Point query for ' + collection['title']),util.createquerylinks(self.server + '/collections/' + c_id + '/latest/position_query_selector','self','position_query_selector','Point query for latest ' + collection['title']),util.createquerylinks(self.server + '/collections/'+cid+'/instances/latest/area','self','area','Area query for ' + collection['title']),util.createquerylinks(self.server + '/collections/'+cid+'/instances/latest/area_query_selector','self','area_query_selector','Area query for ' + collection['title']),util.createquerylinks(self.server + '/collections/'+cid+'/instances/latest/cube','self','cube','Cube query for ' + collection['title']),util.createquerylinks(self.server + '/collections/'+cid+'/instances/latest/cube_query_selector','self','cube_query_selector','Cube query for ' + collection['title'])]}
        description['parameters'] = {}
        for params in col[0]['parameters']:
            description['parameters'][params]={}
            description['parameters'][params]['extent']={}
            description['parameters'][params]['extent']['horizontal']={}
            description['parameters'][params]['extent']['temporal']={'range': col[0]['time']}
        return description




