from html.parser import HTMLParser
import xarray as xr
import datetime
from datetime import timedelta
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent import futures
import time
import pytz
import json
import os
import zarr
import copy
from zarr import blosc
import numpy as np
import pandas as pd
import shutil

BASE_URL = "https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/"
LOCAL_BASE_FOLDER = "/data/ndfd/"
M_WORKERS = 5
LOCATIONS = ['AR.alaska', 'AR.conus', 'AR.guam', 'AR.hawaii', 'AR.puertori']


def requests_retry_session(
    retries=5,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504, 404),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def getLinks(url):
    links = []
    parser = MyHTMLParser()
    r = requests.get(url)
    parser.feed(r.text)
    links = list(parser.get_links())
    parser.reset_links()
    links.pop(0)
    r.close()
    return list(links)


class MyHTMLParser(HTMLParser):

    links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            if attrs[0][1].find('?') == -1:
                self.links.append(attrs[0][1])

    def reset_links(self):
        self.links.clear()

    def get_links(self):
        return self.links


def downloadfile(region, period, name, folderdate):
    if not os.path.exists(LOCAL_BASE_FOLDER + folderdate ):
        os.makedirs(LOCAL_BASE_FOLDER + folderdate)
    if not os.path.exists(LOCAL_BASE_FOLDER + folderdate + "/" + region):
        os.makedirs(LOCAL_BASE_FOLDER + folderdate + "/" +
                    region)
    if not os.path.exists(LOCAL_BASE_FOLDER + folderdate + "/" + region + period):
        os.makedirs(LOCAL_BASE_FOLDER + folderdate + "/" +
                    region + period)
    r = requests_retry_session().get(BASE_URL + region + period + name, stream=True)

    with open(LOCAL_BASE_FOLDER + folderdate + "/" +
                   region + period + name.replace('bin', 'grb'), 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


def extract_date(ds):
    for var in ds.variables:
        if ('initial_time' in ds[var].attrs.keys()) and (not var == "gridrot_0"):
            init_time = pd.to_datetime(ds[var].attrs['initial_time'],
                                       format="%m/%d/%Y (%H:%M)")
            grid_time = None


            time_diff = datetime.datetime.now(pytz.timezone('Europe/London')).utcoffset() * 1e9
            if 'forecast_time0' in ds:
                if 'forecast_time_units' in ds[var].attrs.keys():
                    time_units = str(ds[var].attrs['forecast_time_units'][0])
                    grid_time += np.timedelta64(int(ds[var].attrs['forecast_time0']),
                                                time_units)
                else:
                    if (max(ds['forecast_time0'].values) / np.timedelta64(1, 'h')) > 1000:
                        grid_time = np.datetime64(init_time) + (ds['forecast_time0'].values/60)
                    else:
                        grid_time = np.datetime64(init_time) + ds['forecast_time0'].values
            ds.attrs['init_time'] = init_time.isoformat('T') + 'Z'
            return ds.assign(forecast_time0=grid_time)
    raise ValueError(
        "Time attribute missing")




def create_collections(config_json, folderdate):
    collections={}
    for r in config_json:
        collections[r]={}
        for tp in config_json[r][0]:
            collections[r][tp]=[]
            for p in config_json[r][0][tp]:
                if len(collections[r][tp]) == 0:
                    collection={}
                    collection[p]=config_json[r][0][tp][p]
                    collections[r][tp].append(collection)
                else:
                    try:
                        match_found=False
                        for index in range(0, len(collections[r][tp])):
                            collection=collections[r][tp][index]

                            if ('steps' in config_json[r][0][tp][p]) and ('run_time' in config_json[r][0][tp][p]) and ('steps' in collection[list(collection)[0]]) and ('run_time' in collection[list(collection)[0]]):
                                if (config_json[r][0][tp][p]['run_time'] == collection[list(collection)[0]]['run_time']) and (config_json[r][0][tp][p]['steps'] == collection[list(collection)[0]]['steps']):
                                    collection[p]=config_json[r][0][tp][p]
                                    collections[r][tp][index]=collection
                                    match_found=True
                                    break
                        if not match_found:

                            collection={}
                            collection[p]=config_json[r][0][tp][p]
                            collections[r][tp].append(collection)
                    except TypeError:
                        print("error:{}".format(p))
                    collections['folder'] = folderdate
    return collections


def convert_to_zarr(folderdate):
    compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=blosc.BITSHUFFLE)
    listOfLocations = list(os.listdir(LOCAL_BASE_FOLDER + folderdate + '/'))
    sourcelist = {}
    for location in listOfLocations:
        if location in LOCATIONS:
            listOfPeriods = list(os.listdir(LOCAL_BASE_FOLDER + folderdate +"/"+location))
            sourcelist[location] = {}
            for period in listOfPeriods:
                sourcelist[location][period] = {}
                listOfParameters = list(os.listdir(
                    LOCAL_BASE_FOLDER + folderdate +"/"+location + "/" + period))
                for parameter in listOfParameters:
                    if not parameter == 'zarr':
                        if parameter in sourcelist[location][period]:
                            sourcelist[location][period][parameter].append(
                                LOCAL_BASE_FOLDER + folderdate + '/' + location + "/" + period + "/" + parameter)
                        else:
                            sourcelist[location][period][parameter] = []
                            sourcelist[location][period][parameter].append(
                                LOCAL_BASE_FOLDER + folderdate + '/' + location + "/" + period + "/" + parameter)
    metadata = {}
    for location in sourcelist:
        pMeta = {}
        metadata[location] = []
        for period in sourcelist[location]:
            if not os.path.exists(LOCAL_BASE_FOLDER + folderdate +"/"+location + "/" + period + "/zarr/"):
                os.makedirs(LOCAL_BASE_FOLDER + folderdate +"/"+location +
                            "/" + period + "/zarr/")
            pMeta[period] = {}
            for parameter in sourcelist[location][period]:
                if (parameter.find('.idx') == -1) and (parameter.find('ls-l') == -1) and (parameter.find('.zarr') == -1):

                    try:
                        ds = xr.open_mfdataset(
                            sourcelist[location][period][parameter], concat_dim='forecast_time0',  preprocess=extract_date,  engine='pynio')
                    except Exception as e:
                        print('first err:{}'.format(e))
 
                    cf = list(ds.data_vars.keys())[0]

                    the_encoding = {cf: {'compressor': compressor}}

                    coordref = {}
                    coordref['file'] = parameter
                    coordref['run_time'] = ds.attrs['init_time']
                    coordref['long_name'] = ds[cf].long_name
                    coordref['grib2_code'] = ds[cf].parameter_template_discipline_category_number.tolist()
                    coordref['units'] = ds[cf].units
                    if 'level' in ds[cf]:
                        coordref['level'] = {}
                        coordref['level']['type'] = ds[cf].level_type
                        coordref['level']['values'] = ds[cf].level.tolist()

                    try:
                        coordref['steps'] = ds.forecast_time0.data.tolist()
                    except Exception as e:
                        print(e)

                    try:
                        coordref['lon'] = {}
                        coordref['lon']['Dx'] = ds.coords['gridlon_0'].attrs['Dx'].data[0]
                        coordref['lon']['Dy'] = ds.coords['gridlon_0'].attrs['Dy'].data[0]
                        coordref['lon']['La1'] = ds.coords['gridlon_0'].attrs['La1'].data[0]
                        coordref['lon']['Lo1'] = ds.coords['gridlon_0'].attrs['Lo1'].data[0]
                        coordref['lon']['Lov'] = ds.coords['gridlon_0'].attrs['Lov'].data[0]
                        coordref['lon']['corners'] = [ds.coords['gridlon_0'].attrs['corners'].data[0], ds.coords['gridlon_0'].attrs['corners'].data[1],
                        ds.coords['gridlon_0'].attrs['corners'].data[2], ds.coords['gridlon_0'].attrs['corners'].data[3]]
                        coordref['lon']['grid_type'] = ds.coords['gridlon_0'].attrs['grid_type']
                        coordref['lon']['long_name'] = ds.coords['gridlon_0'].attrs['long_name']
                        coordref['lon']['units'] = ds.coords['gridlon_0'].attrs['units']
                        coordref['lat'] = {}
                        coordref['lat']['Dx'] = ds.coords['gridlat_0'].attrs['Dx'].data[0]
                        coordref['lat']['Dy'] = ds.coords['gridlat_0'].attrs['Dy'].data[0]
                        coordref['lat']['La1'] = ds.coords['gridlat_0'].attrs['La1'].data[0]
                        coordref['lat']['Lo1'] = ds.coords['gridlat_0'].attrs['Lo1'].data[0]
                        coordref['lat']['Lov'] = ds.coords['gridlat_0'].attrs['Lov'].data[0]
                        coordref['lat']['corners'] = [ds.coords['gridlat_0'].attrs['corners'].data[0], ds.coords['gridlat_0'].attrs['corners'].data[1],
                        ds.coords['gridlat_0'].attrs['corners'].data[2], ds.coords['gridlat_0'].attrs['corners'].data[3]]
                        coordref['lat']['grid_type'] = ds.coords['gridlat_0'].attrs['grid_type']
                        coordref['lat']['long_name'] = ds.coords['gridlat_0'].attrs['long_name']
                        coordref['lat']['units'] = ds.coords['gridlat_0'].attrs['units']
                        try:
                            ds = ds.drop(['gridrot_0'])
                        except ValueError as ve:
                            print (parameter + ' :' + str(ve))
                    except KeyError:
                        coordref['lon'] = {}
                        coordref['lon']['Di'] = ds.coords['lon_0'].attrs['Di'].data[0]
                        coordref['lon']['Dj'] = ds.coords['lon_0'].attrs['Dj'].data[0]
                        coordref['lon']['La1'] = ds.coords['lon_0'].attrs['La1'].data[0]
                        coordref['lon']['La2'] = ds.coords['lon_0'].attrs['La2'].data[0]
                        coordref['lon']['LaD'] = ds.coords['lon_0'].attrs['LaD'].data[0]
                        coordref['lon']['Lo1'] = ds.coords['lon_0'].attrs['Lo1'].data[0]
                        coordref['lon']['Lo2'] = ds.coords['lon_0'].attrs['Lo2'].data[0]
                        coordref['lon']['grid_type'] = ds.coords['lon_0'].attrs['grid_type']
                        coordref['lon']['long_name'] = ds.coords['lon_0'].attrs['long_name']
                        coordref['lon']['units'] = ds.coords['lon_0'].attrs['units']
                        coordref['lat'] = {}
                        coordref['lat']['Di'] = ds.coords['lat_0'].attrs['Di'].data[0]
                        coordref['lat']['Dj'] = ds.coords['lat_0'].attrs['Dj'].data[0]
                        coordref['lat']['La1'] = ds.coords['lat_0'].attrs['La1'].data[0]
                        coordref['lat']['La2'] = ds.coords['lat_0'].attrs['La2'].data[0]
                        coordref['lat']['LaD'] = ds.coords['lat_0'].attrs['LaD'].data[0]
                        coordref['lat']['Lo1'] = ds.coords['lat_0'].attrs['Lo1'].data[0]
                        coordref['lat']['Lo2'] = ds.coords['lat_0'].attrs['Lo2'].data[0]
                        coordref['lat']['grid_type'] = ds.coords['lat_0'].attrs['GridType']
                        coordref['lat']['long_name'] = ds.coords['lat_0'].attrs['long_name']
                        coordref['lat']['units'] = ds.coords['lat_0'].attrs['units']

                    try:
                        if location == "AR.conus":
                            ds['projection_x_coordinate'] = xr.DataArray((ds.xgrid_0 * (ds.gridlon_0.attrs['Dx'][0] *1000)) + (-2764474.3507319884374738))
                            ds['projection_x_coordinate'].attrs = copy.deepcopy(ds.gridlon_0.attrs)
                            ds['projection_x_coordinate'].attrs['min_x'] = -2764474.3507319884374738
                            ds['projection_y_coordinate'] = xr.DataArray((ds.ygrid_0 * (ds.gridlat_0.attrs['Dy'][0] * 1000)) + (-265059.3202076056040823))
                            ds['projection_y_coordinate'].attrs = copy.deepcopy(ds.gridlat_0.attrs)
                            ds['projection_y_coordinate'].attrs['min_y'] = -265059.3202076056040823

                            ds = ds.assign_coords(xgrid_0=ds.projection_x_coordinate)          
                            ds = ds.assign_coords(ygrid_0=ds.projection_y_coordinate)          

                            ds = ds.drop(['gridlon_0'])
                            ds = ds.drop(['gridlat_0'])
                        ds.to_zarr(LOCAL_BASE_FOLDER + folderdate + "/" + location + "/" + period + "/zarr/" +
                                   parameter.replace('grb', 'zarr'), encoding=the_encoding)
                    except Exception as e:
                        print(location + ': ' + parameter + ':' + str(e))

                pMeta[period][cf] = copy.deepcopy(coordref)
        metadata[location].append(copy.deepcopy(pMeta))
    with open(LOCAL_BASE_FOLDER + "/coord_info.json", 'w') as f:
        f.write(json.dumps(create_collections(metadata, folderdate)))
    
    folders = list(os.listdir(LOCAL_BASE_FOLDER))
    for old in folders:
        if old.find('20') > -1:
            if not old == folderdate: 
                shutil.rmtree(LOCAL_BASE_FOLDER + '/' + old)

if __name__ == '__main__':
    folderdate = datetime.datetime.now().strftime("%Y-%m-%d%H%M")
    regions=getLinks(BASE_URL)
    for region in regions:
        if region[:-1] in LOCATIONS:
            periods=getLinks(BASE_URL + region)
            for period in periods:
                params=getLinks(BASE_URL + region + period)
                for param in params:
                    downloadfile(region, period, param, folderdate)
    convert_to_zarr(folderdate)     
