# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2018 Tom Kralidis
#
#  Modified by Shane Mill 6/2/21 for the purposes of prototyping
#  a provider for Himawari L2 Satellite Data
#
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================
import cartopy.crs as ccrs
import copy
from datacube.utils.cog import write_cog
import logging
from flask import request as rq
import flask
import json
import metpy
import numpy as np
import os
import pathlib
from pyproj import CRS
import xarray as xr
import uuid as ud
import zipfile
from scipy.interpolate import griddata

LOGGER = logging.getLogger(__name__)


class HimawariProvider(object):
    def __init__(self, dataset, config):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: pygeoapi.providers.base.BaseProvider
        """
        self.config = config
        self.DATASET_FOLDER = config['datasets'][dataset]['provider']['data_source']
        self.dir_root=self.DATASET_FOLDER
        self.ps_cov= {
            "domain": {
                "axes": {
                   "t": {
                      "values": [
                       ]
                    },
                    "x": {
                       "values": [
                       ]
                    },
                    "y": {
                       "values": [
                       ]
                    },
                },
                "domainType": "PointSeries",
                "referencing": [
                {
                   "coordinates": [
                       "y",
                       "x"
                   ],
                   "system": {
                       "id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
                       "type": "GeographicCRS"
                   }
                },
                {
                    "coordinates": [
                       "t"
                    ],
                    "system": {
                       "calendar": "Gregorian",
                       "type": "TemporalRS"
                    }
                }
                ],
                "type": "Domain"
            },
            "parameters": {
               "p1": {
                  "attrs": {
                  },
                  "description": {
                      "en": ""
                   },
                   "observedProperty": {
                      "label": {
                          "en": ""
                       }
                   },
                   "unit": {
                      "label": {
                          "en": ""
                      },
                      "symbol": {
                          "type": "",
                          "value": ""
                      }
                   }
               }
            },
            "ranges": {
               "p1": {
                  "axisNames": [
                  ],
                  "dataType": "float",
                  "shape": [
                  ],
                  "type": "NdArray",
                  "values": [
                   ]
               }
            },
            "type": "Coverage"
        }
        self.area_cov= {
            "domain": {
                "axes": {
                   "t": {
                      "values": [
                       ]
                    },
                    "x": {
                       "values": [
                       ]
                    },
                    "y": {
                       "values": [
                       ]
                    },
                },
                "domainType": "Grid",
                "referencing": [
                {
                   "coordinates": [
                       "y",
                       "x"
                   ],
                   "system": {
                       "id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
                       "type": "GeographicCRS"
                   }
                },
                {
                    "coordinates": [
                       "t"
                    ],
                    "system": {
                       "calendar": "Gregorian",
                       "type": "TemporalRS"
                    }
                }
                ],
                "type": "Domain"
            },
            "parameters": {
               "p1": {
                  "attrs": {
                  },
                  "description": {
                      "en": ""
                   },
                   "observedProperty": {
                      "label": {
                          "en": ""
                       }
                   },
                   "unit": {
                      "label": {
                          "en": ""
                      },
                      "symbol": {
                          "type": "",
                          "value": ""
                      }
                   }
               }
            },
            "ranges": {
               "p1": {
                  "axisNames": [
                  ],
                  "dataType": "float",
                  "shape": [
                  ],
                  "type": "NdArray",
                  "values": [
                   ]
               }
            },
            "type": "Coverage"
        }


    def query(self, dataset, qtype, coords, time_range, z_value, params, instance, outputFormat):
          self.uuid=str(ud.uuid4().hex)
          zarr_ds = self.config['datasets'][dataset]['provider']['data_source']+'/zarr'
          ds = xr.open_zarr(zarr_ds)
          if qtype=='point':
             output, output_boolean = self.get_position_data(ds,coords,qtype,params,time_range,outputFormat)
             return output, output_boolean
          if qtype=='polygon':
             output, output_boolean = self.get_polygon_data(ds,coords,qtype,params,time_range,outputFormat)
             return output, output_boolean


    def get_position_data(self,ds,coords,qtype,params,time_range,outputFormat):
          lon = coords[0] # longitude of interest
          lat = coords[1] # latitude of interest
          sat_height=ds.goes_imager_projection.attrs['perspective_point_height'][0]
          sweep_angle_axis=ds.goes_imager_projection.attrs['sweep_angle_axis']
          cen_lon=ds.geospatial_lat_lon_extent.geospatial_lon_center[0] 
          data_crs = ccrs.Geostationary(central_longitude=cen_lon,satellite_height=sat_height, false_easting=0, false_northing=0, globe=None, sweep_axis='x')
          x, y = data_crs.transform_point(lon, lat, src_crs=ccrs.PlateCarree())
          new_proj_attrs={}
          for key, value in ds.goes_imager_projection.attrs.items():
             if type(value)==list:
                new_proj_attrs.update({key: value[0]})
             else:
                new_proj_attrs.update({key: value})
          cc=CRS.from_cf(new_proj_attrs)
          ds.coords["x"] = ds.x
          ds.coords["y"] = ds.y
          ds.coords["goes_imager_projection"] = ds.goes_imager_projection
          ds.coords["time"] = ds.time
          ds.rio.write_crs(cc, inplace=True)
          ds=ds.assign_coords({'x':ds.x.values * sat_height})
          ds=ds.assign_coords({'y':ds.y.values * sat_height})
          output = ds.sel(x=lon,y=lat, method='nearest')
          output=output[params]
          j_output = output.to_dict()
          if outputFormat=='CoverageJSON':
             j_cov = self.to_covjson(j_output,qtype,lat,lon)
             return json.dumps(j_cov, indent=4, sort_keys=True, default=str).replace('NaN', 'null'), 'no_delete'


    def get_polygon_data(self,ds,coords,qtype,params,time_range,outputFormat):
          output=ds[params]
          output=output.sel({'time':slice(str(time_range[0]),str(time_range[1]))})
          geometries=[];coord_list=list()
          output=ds[params]
          if len(coords) == 5:
             coords_clip=[[coords[0][0],coords[0][1]],[coords[1][0],coords[1][1]],[coords[2][0]-1,coords[2][1]],[coords[3][0]-1,coords[3][1]],[coords[4][0],coords[4][1]]]
          else:
             coords_clip=coords
          geometries.append({'type':'Polygon', 'coordinates':[coords_clip]})
          output=output.rio.write_crs(4326)
          output=output.rio.clip(geometries,output.rio.crs)
          j_output = output.to_dict()
          if outputFormat=='CoverageJSON':
             j_cov = self.to_covjson(j_output,qtype)
             return json.dumps(j_cov, indent=4, sort_keys=True, default=str).replace('NaN', 'null'), 'no_delete'
          if outputFormat=="COGeotiff":
             f_location,zip_bool=export_geotiff(self,output)
             if zip_bool==False:
                return flask.send_from_directory(self.dir_root,self.uuid+'.tif',as_attachment=True), self.dir_root+'/'+self.uuid+'.tif'
             if zip_bool==True:
                root=self.dir_root+'/temp_dir/'
                zip_file=f_location.split('/')[-1]+'.zip'
                return flask.send_from_directory(root,zip_file,as_attachment=True), 'no_delete'
          if outputFormat=="NetCDF":
             for data_vars in output.data_vars:
                del output[data_vars].attrs['grid_mapping']
             conversion=output.to_netcdf(self.dir_root+'/output-'+self.uuid+'.nc')
             return flask.send_from_directory(self.dir_root,'output-'+self.uuid+'.nc',as_attachment=True), self.dir_root+'/output-'+self.uuid+'.nc'





    def to_covjson(self,j_output,qtype):
          if qtype == 'point':
             cov = self.ps_cov
          if qtype=='polygon':
             cov = self.area_cov
          new_output=copy.deepcopy(cov)
          new_output['domain']['axes']['t']['values']=copy.deepcopy(j_output['coords']['time']['data'])
          time_list=list()
          for t in j_output['coords']['time']['data']:
             time_list.append(t.isoformat())
          new_output['domain']['axes']['t']['values']=time_list
          try:
             new_output['domain']['axes']['x']['values']=copy.deepcopy(j_output['coords']['x']['data'])
             new_output['domain']['axes']['y']['values']=copy.deepcopy(j_output['coords']['y']['data'])
          except:
             new_output['domain']['axes']['x']['values']=copy.deepcopy(j_output['coords']['lon']['data'])
             new_output['domain']['axes']['y']['values']=copy.deepcopy(j_output['coords']['lat']['data'])
          for p in j_output['data_vars']:
             new_output['parameters'][p]={}
             new_output['parameters'][p]=copy.deepcopy(new_output['parameters']['p1'])
             new_output['parameters'][p]['description']=[p]
             try:
                new_output['parameters'][p]['observedProperty']['label']['en']=p
                new_output['parameters'][p]['unit']['label']['en']=p
                new_output['parameters'][p]['unit']['symbol']={'value': copy.deepcopy(j_output['data_vars'][p]['attrs']['units'])}
             except:
                pass
             new_output['ranges'][p]=copy.deepcopy(new_output['ranges']['p1'])
             if qtype=='point': 
                new_output['ranges'][p]['values']=copy.deepcopy(j_output['data_vars'][p]['data'])
                new_output['ranges'][p]['shape']=[np.array(j_output['data_vars'][p]['data']).shape[0],1,1]
             if qtype=='polygon':
                new_output['ranges'][p]['values']=copy.deepcopy(np.array(j_output['data_vars'][p]['data']).flatten().tolist())
                new_output['ranges'][p]['shape']=np.array(j_output['data_vars'][p]['data']).shape
             new_output['ranges'][p]['axisNames']=['t','y','x']
          del new_output['parameters']['p1']
          del new_output['ranges']['p1']
          return new_output


def export_geotiff(self,output):
   dim_tracker={}
   zip_bool=False
   for dims in output.dims:
      if 'time' in dims:
         if output.dims[dims] == 1:
            output=output.sel({dims: output[dims].values[0]})
         else:
            dim_tracker[dims]=output[dims].values.tolist()
   #for forecast time selection I need to use np.timedelta64
   if len(dim_tracker)==0:
      f_location=self.dir_root+'/'+self.uuid+'.tif'
      output_array=output.to_array()
      output_array=output_array.compute()
      df=write_cog(output_array,fname=f_location)
   else:
      f_location=self.dir_root+'/temp_dir/'+self.uuid
      os.makedirs(f_location)
      if len(dim_tracker.keys())==1:
         if 'time' in list(dim_tracker.keys())[0]:
            for element in dim_tracker[list(dim_tracker.keys())[0]]:
               sample=output.sel({'time':element})
               sample_array=sample.to_array()
               sample_array=sample_array.compute()
               fname=str(element)+'.tif'
               w_location=f_location+'/'+fname
               df=write_cog(sample_array,fname=w_location)
      #create zip
      zip_bool=True
      base_path = pathlib.Path(f_location+'/')
      with zipfile.ZipFile(f_location+'.zip', mode='w') as z:
         for f_name in base_path.iterdir():
            z.write(f_name)
   return f_location, zip_bool

