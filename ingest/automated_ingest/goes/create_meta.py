import xarray as xr
import json

def meta(location,collection_name):
   ds=xr.open_zarr(location)
   ds_dict=ds.to_dict(data=False)
   col_dict={}
   col_dict['collection_name']='latest_'+collection_name
   dim_list=list(ds_dict['dims'].keys())
   dim_list.append('time')
   col_dict['dimensions']=dim_list
   col_dict['parameters']=list(ds_dict['data_vars'].keys())
   long_name=list()
   for dv in ds.data_vars:
      long_name.append(dv)
   col_dict['long_name']=long_name
   forecast_time_list=list()
   for dim in ds.dims:
      if dim=='time':
         ftime=ds[dim].values.tolist()
         for t in ftime:
            forecast_time_list.append(t)
   forecast_time_list = list(dict.fromkeys(forecast_time_list))
   col_dict['time']=sorted(forecast_time_list)
   for d in col_dict['dimensions']:
      if 'time' not in d:
         col_dict[d]=ds[d].values.tolist()
   with open(location+'_collection.json', 'w+') as json_final:
      json_list=list()
      json_list=[col_dict]
      json.dump(json_list,json_final,indent=2, sort_keys=True)
   return


if __name__ == "__main__":
   meta(location,collection_name)
